# ADR — Arquitetura de autenticação e gestão de usuários

> Status: **Proposta** · Data: 2026-07-16 · Escopo: authn/authz e ciclo de vida de usuários do Malachi
>
> Guia irmão (práticas, não arquitetura): [SECURITY_DEVELOPMENT.md](SECURITY_DEVELOPMENT.md) ·
> Design geral do port: [NORTHGUARD_PORT.md](NORTHGUARD_PORT.md)

Este documento **não** implementa nada — registra o estado atual, compara com concorrentes e o NorthGuard,
e propõe um roadmap faseado. A ordem/execução das fatias é decidida em conversas futuras.

---

## 1. Contexto

O Malachi mira ser um produto **vendável**: escalável, seguro e fácil de operar. Duas perguntas
estratégicas motivaram esta análise:

1. É aceitável ter **usuários/senhas padrão hardcoded no código**?
2. **Mnesia/ETS** é a forma certa de gerenciar usuários (produtores, consumidores, admin) num produto
   **distribuído**?

A resposta curta: (1) não — é um anti-padrão que já tem mitigações, mas deveria ser eliminado; (2) o
problema não é "Mnesia vs ETS" em si, e sim que a **implementação atual não replica os usuários entre nós**
— um gap arquitetural real para um sistema de cluster.

---

## 2. Estado atual (o que o código faz hoje)

Todo o stack de auth é **Malachi-original** (o NorthGuard não descreve auth — ver §4). Vive em
`lib/malachi/auth.ex` + `lib/malachi/auth/{user_store,session_manager,lockout_manager,config_validator}.ex`,
fiado na árvore em `lib/malachi/application.ex`.

### O que já está bem-feito (reconhecer antes de criticar)
- **Senhas hasheadas com Argon2** — nunca em texto puro no banco (`lib/malachi/auth.ex:302` hash,
  `auth.ex:306` verify; dep `{:argon2_elixir, "~> 4.1.3"}` em `mix.exs:52`).
- **Mitigação de timing** — usuário inexistente ainda roda `Argon2.no_user_verify()` (`auth.ex:80`).
- **Guard de produção** — em `:prod` o boot **falha** (`config/runtime.exs:308` `raise`) se você não
  sobrescrever os defaults; `ConfigValidator` mantém uma blacklist de senhas fracas
  (`config_validator.ex:29-40`).
- **Sessões endurecidas** — tokens de 32 bytes aleatórios, IP-binding por padrão, expiração, detecção de
  hijack (`lib/malachi/auth/session_manager.ex`).

### As lacunas
- **Storage node-local, não replicado (crítico).** Mnesia `disc_copies` + cache ETS
  (`lib/malachi/auth/user_store.ex`). A tabela é criada só em `[node]` (`user_store.ex:337` `create_schema`,
  `user_store.ex:350` `{storage_type, [node]}`, `user_store.ex:353` `create_table`); **não há**
  `add_table_copy`/`extra_db_nodes` em lugar nenhum. Consequência: **cada nó tem seu próprio user store**; um
  nó novo **re-semeia do config**, não sincroniza. Um usuário criado no nó A **não existe** no nó B.
- **Credenciais padrão hardcoded.** `admin/admin123`, `producer/producer123`, `consumer/consumer123`,
  `app/app123` em texto puro em `config/config.exs:14-19` (hasheadas só ao semear). Fresta: o mínimo de 12
  caracteres só é exigido se `require_strong_passwords` estiver ligado — e o default é **false**
  (`runtime.exs:361`); dev/staging rodam com `admin123`.
- **Sem gestão em runtime.** Só funções in-process `Auth.add_user/remove_user/change_password/list_users`
  (`auth.ex:146-167`). **Não há CLI, endpoint HTTP nem op de wire** para CRUD de usuário. Como o Mnesia não
  replica, uma mudança via RPC/IEx cai **num nó só**.
- **AutZ grosso e global.** Três permissões `:admin`/`:produce`/`:consume` (`auth.ex:144`), impostas no
  boundary TCP (`lib/malachi/tcp_protocol.ex:196-204`) e no dashboard (`lib/malachi/dashboard.ex:245-268`).
  **Sem ACL por-tópico e sem multi-tenancy.**
- **Sessões e lockouts voláteis.** ETS puro — somem no restart e não cruzam nós
  (`session_manager.ex`, `lockout_manager.ex:18-20`).
- **Sem auth externa.** Nenhum SASL/OIDC/JWT/LDAP/token. mTLS é opt-in mas **verifica sem autenticar**: um
  cert de cliente válido **não** é mapeado para usuário/permissão — a auth por senha ainda roda
  (`lib/malachi/tcp_acceptor_pool.ex:111-118`; `lib/malachi/tls_validator.ex` só valida o cert do servidor).

---

## 3. Problemas e decisões

Cada item segue o padrão: problema concreto → opções (esforço/risco) → recomendação → como os outros tratam.

### P1 — Credenciais padrão hardcoded no código
**Problema:** senhas em texto puro versionadas (`config/config.exs:14-19`). Anti-padrão OWASP/CWE-798. As
mitigações de prod ajudam, mas o padrão persiste e dev/staging ficam expostos.

- **(a) Eliminar defaults — gerar admin aleatório no 1º boot e logar 1x.** Padrão de Postgres/Elasticsearch/
  Redpanda. *Esforço*: baixo-médio. *Risco*: baixo. *Impacto*: primeiro contato do operador muda (lê o log).
- **(b) Exigir provisionamento explícito** (nenhum usuário até criar via CLI/env). *Esforço*: médio.
  *Risco*: baixo. *Fricção*: maior no onboarding.
- **(c) Manter, só endurecer o gating** (ligar `require_strong_passwords` por padrão, defaults fracos só em
  `:dev`/`:test`). *Esforço*: mínimo. *Risco*: o padrão "credencial no código" continua.

**Recomendação: (a) + endurecer o gating de (c).** Elimina o segredo versionado com a menor fricção; defaults
fracos só em `:dev`/`:test`. **Concorrentes:** nenhum embarca senha fixa; Redpanda/ES imprimem credencial
gerada no 1º boot. **NorthGuard:** não aplicável (delega à plataforma).

### P2 — User store node-local (não replicado) — o gap arquitetural
**Problema:** usuários não são consistentes no cluster (§2). Num produto distribuído isso está **quebrado**.

- **(a) Mover usuários para o control plane `ra`** (Raft) já existente. Usuários são "metadado global,
  pequeno, crítico, consistente" — exatamente o que o `ra` do Malachi já replica (metadata/vnodes + lease).
  *Esforço*: médio-alto. *Risco*: médio. *Ganho*: replicação/consistência + reuso de bootstrap/failover.
- **(b) Replicar o Mnesia de verdade** (`add_table_copy`, `disc_copies` multi-nó — como o RabbitMQ).
  *Esforço*: médio. *Risco*: médio-alto (netsplit/merge/backup do Mnesia; um 2º mecanismo de replicação
  além do `ra`). *Ganho*: menos reescrita.
- **(c) Manter node-local.** *Rejeitar* — não serve a um produto de cluster.

**Recomendação: (a).** Alinha com a arquitetura (um só quórum replicado para todo metadado crítico) e é
como Kafka/Redpanda fazem. **Concorrentes:** Kafka/Redpanda guardam credenciais **no próprio log de metadata
replicado** (KRaft/controller Raft); RabbitMQ usa Mnesia **replicado**. **NorthGuard:** silente.

### P3 — Sem superfície de gestão em runtime
**Problema:** não há CLI/API/wire para CRUD e rotação de usuário (`auth.ex:146-167` só in-process).

- **(a) CLI administrativo + admin API/wire-op** (create/update/delete/rotate/list). *Esforço*: médio.
  *Risco*: baixo (depende de P2 para valer no cluster). *Ganho*: operabilidade real.
- **(b) Só CLI** (sem API). *Esforço*: baixo. *Limite*: automação/integração mais difícil.
- **(c) Manter config/env no boot.** *Rejeitar* para produto — rotação exige restart e não cobre o cluster.

**Recomendação: (a), depois de P2.** **Concorrentes:** `rabbitmqctl`+HTTP API (RabbitMQ), CLI+Admin API
(Kafka), REST admin (Pulsar), `nsc` (NATS). **NorthGuard:** silente.

### P4 — Auth externa ausente (table-stakes de vendabilidade)
**Problema:** só username/password interno; nenhum IdP externo; mTLS verifica mas não autentica.

- **(a) Definir plug points** — mapear identidade de **mTLS→usuário** e interfaces plugáveis para **OIDC/JWT**
  e **LDAP** (definir o **contrato**, implementar 1 provider de referência). *Esforço*: médio-alto.
  *Risco*: médio. *Ganho*: o cliente pluga o IdP dele.
- **(b) Implementar um mecanismo específico** (só OIDC, por ex.). *Esforço*: médio. *Limite*: acopla a um IdP.
- **(c) Nada.** *Limite*: barra a venda enterprise.

**Recomendação: (a) — contrato plugável primeiro, mTLS-identidade como 1º provider.** **Concorrentes:** todos
plugáveis (Kafka SASL/OAuth/Kerberos; Pulsar JWT/OAuth2/TLS; RabbitMQ LDAP/OAuth2; NATS JWT descentralizado).
**NorthGuard:** delega à plataforma da LinkedIn (mTLS de serviço + authz central) — um produto OSS não pode
assumir essa plataforma, por isso precisa dos plug points.

### P5 — AutZ grosso / sem multi-tenancy
**Problema:** RBAC global de 3 permissões; sem ACL por-tópico nem isolamento por tenant.

- **(a) ACL por-recurso (tópico/grupo) + tenants/vhosts.** *Esforço*: alto. *Risco*: médio-alto (toca o
  boundary e o metadata). *Ganho*: isolamento real (multi-tenant).
- **(b) Só ACL por-tópico** (sem tenant). *Esforço*: médio. *Ganho*: parcial.
- **(c) Manter global.** OK para single-tenant; barra multi-tenant.

**Recomendação: (a), faseado depois de P2-P4.** **Concorrentes:** ACLs por-recurso (Kafka/Redpanda),
vhosts (RabbitMQ), tenant→namespace→topic (Pulsar), accounts (NATS). **NorthGuard:** silente.

### P6 — Sessões e lockouts voláteis
**Problema:** ETS puro → perdidos no restart, não cruzam nós (`lockout_manager.ex:18-20`).

- **(a) Persistir/replicar** (junto de P2, no mesmo mecanismo). *Esforço*: médio. *Ganho*: lockout e sessão
  sobrevivem a restart/failover.
- **(b) Só persistir local.** *Esforço*: baixo. *Limite*: não cobre o cluster.
- **(c) Manter volátil.** Aceitável a curto prazo; lockout reinicia no restart (janela de brute-force).

**Recomendação: (a), encaixado em P2 — secundário.**

---

## 4. Como os concorrentes (e o NorthGuard) resolvem

| Sistema | AutN | Onde vivem credenciais + ACLs | AutZ / multi-tenancy | Gestão |
|---|---|---|---|---|
| **Kafka / Redpanda** | SASL (SCRAM/Kerberos/OAUTHBEARER) + mTLS | **no log de metadata replicado** (KRaft / controller Raft) | ACL por-recurso; authorizer plugável | CLI + Admin API |
| **RabbitMQ** | interno (**Mnesia**, igual ao Malachi) + LDAP/OAuth2/JWT/HTTP plugáveis | Mnesia **replicado** | permissões por **vhost** (multi-tenant) + topic authz | `rabbitmqctl` + HTTP API + UI |
| **Pulsar** | plugável: JWT, OAuth2, TLS, Kerberos, Athenz | ZooKeeper/etcd | **multi-tenancy first-class** (tenant→namespace→topic) | REST admin + CLI |
| **NATS** | **JWT descentralizado** (assinado por operator) + nkeys | nos próprios JWTs (sem DB central) | **accounts** (multi-tenant) | `nsc` CLI |
| **NorthGuard** | *não descrito* — delega à plataforma da LinkedIn (mTLS de serviço + authz central) | — | — | — |

**Lições:** (1) credenciais + ACLs no **mesmo quórum replicado** do resto do metadata (KRaft/Redpanda) →
valida **P2** (mover usuários para o `ra`). (2) Auth **plugável** é table-stakes → **P4**. (3) Multi-tenancy
+ ACL por-recurso separa "brinquedo" de "vendável" → **P5**. (4) Gestão em runtime (CLI+API) é obrigatória →
**P3**. Sobre o **NorthGuard**: em escala interna, auth é problema de *plataforma*, não uma tabela de
usuários por-serviço — um produto OSS não pode assumir isso, então oferece os plug points no lugar.

---

## 5. Roadmap faseado recomendado

1. **Fase 1 — Segurança (P1). ✅ Feito (decisão 1A).** Removidas as credenciais fracas versionadas dos três
   pontos (`config/config.exs`, os fallbacks `|| "admin123"` do `config/runtime.exs`, o fallback do
   `seed_default_users` em `lib/malachi/auth.ex`). O config base semeia `[]`; os defaults de conveniência
   ficam só em `config/dev.exs` e `config/test.exs` (nunca no caminho de prod); prod **exige senha explícita**
   via env (`*_PASS` ou `MALACHIMQ_DEFAULT_USERS`). A Fase 1 escolheu **1A (exigir senha explícita, `raise`)**
   como interino porque, sem o P2 (replicação), um admin gerado divergiria por-nó.
   `require_strong_passwords` deixado como está (ortogonal).
   **✅ Promovido para generate-random depois do P2 (decisão 1A+2A):** o `raise` foi **substituído** por gerar
   um **admin aleatório** no 1º boot e logá-lo **uma vez** (padrão Redpanda/ES). Cluster-safe: cada nó gera e
   chama `put_user` por consenso, o `:user_exists` do `ra` deduplica → exatamente **um** password vence e é
   logado. Só o admin é gerado (escopo 2A); producer/consumer/app são semeados só se configurados. Flag
   `:generate_admin` (setada no `runtime.exs` quando não há `MALACHIMQ_ADMIN_PASS`); `Auth.generate_admin_if_absent/1`
   gera+semeia+loga; `ConfigValidator` ciente da flag. `MALACHIMQ_DISABLE_DEFAULT_USERS` é o opt-out.
   *Tradeoff:* a senha aparece no log (o operador deve protegê-lo / rotacionar) — padrão da indústria.
2. **Fase 2 — Replicação no `ra` (P2) + persistência de sessão/lockout (P6). 🚧 Em andamento (decisão 1A).**
   Move os usuários do Mnesia node-local para um **cluster `ra` dedicado** (`UserMachine` sobre a máquina pura
   `UserRegistry`), espelhando o par `Lease`/`LeaseMachine`: escritas por consenso, leituras do replica local
   (o replica é o cache — sem sync de ETS entre nós). Greenfield (dropa o Mnesia). Confirmado escalável ao
   nível NorthGuard: usuários são metadado global small-data/rare-write/local-read — um único grupo Raft é o
   home certo (KRaft/Redpanda), o eixo que escala (throughput/nós) é o data plane já shardado; escala de
   identidade extrema fica pro P4 (IdP externo). Sub-fatiado: **P2-1 ✅** (`Malachi.Auth.UserRegistry` puro:
   `put_user`/`delete_user`/`update_password`/`import_users` + queries, timestamps do `meta.system_time`,
   catch-all defensivo; 11 testes) → **P2-2 ✅** (`UserMachine` ra_machine alimentando o `meta.system_time` +
   `UserServer` start/reconcile/comandos com **leituras via `:ra.local_query`** no replica local; testado que
   um usuário escrito num nó é legível no replica local de **outro** nó — o que o Mnesia não fazia — e que o
   store commita após perder um membro (HA)) → **P2-3 ✅** (`UserStore` religado como fachada **stateless** sobre
   o `UserServer`, preservando a API pública — `Auth`/testes agnósticos ao backend; `Auth` lê via
   `UserStore.get_user` (→ `:ra.local_query`) em vez do ETS, **removido**; o app sobe `ra` **sempre**
   (single-node incluso) e forma o cluster `LogUsers` antes do `Auth`, que semeia os defaults por consenso
   (idempotente, com retry pra janela de quórum multi-node); **Mnesia dropado** do `extra_applications`.
   Fricção de teste resolvida: o app é dono do `ra`, então os testes de cluster não chamam mais `:ra.start_in`
   — que **reinicia** o `ra` e mataria o `LogUsers` — e a distribuição sobe no `test_helper` com nome de nó
   estável). **P2 completo — usuários replicados no cluster.** *Resta P6 (persistir sessões/lockouts), adiado.*
   *Fundação para P3-P5.*
3. **Fase 3 — Gestão em runtime (P3). 🚧 Em andamento (as 3 superfícies, fatiadas).** As três compartilham as
   mesmas `Auth.*` (que já vão pro `ra`). **P3-1 ✅ — ops de wire admin-gated:** novos `api_key`s no protocolo
   binário (`create_user`=8, `delete_user`=9, `change_password`=10, `list_users`=11), handlers no
   `tcp_protocol` embrulhados em `with_permission(session, :admin, ...)` chamando `Auth.add_user`/`remove_user`/
   `change_password`/`list_users`; codecs no `Wire` (permissões como strings, mapeadas de volta pros átomos
   permitidos com validação → `:invalid_permissions`); senhas cruzam a rede em claro (como o handshake) → TLS
   em prod. Modelo Kafka AdminClient. Testado end-to-end (`log_protocol_test`): admin cria um usuário que
   autentica + usa a permissão + é listado, delete revoga; troca de senha; não-admin negado; permissão inválida
   rejeitada. → **P3-2 ✅ — CLI Node sobre as ops:** métodos `createUser`/`deleteUser`/`changePassword`/
   `listUsers` no `MalachiClient` (+ codecs no `scripts/lib/wire.js`) e um CLI `scripts/user.js`
   (`list`/`create`/`passwd`/`delete`, default admin/admin123). Validado **end-to-end contra o servidor real**
   (boot em dev → list/create/passwd/delete via CLI, não-admin negado, help) — sem harness de teste JS (padrão
   das fatias de cliente Node). → **P3-3 ⏳** (API REST admin no dashboard) → **P3-4 ⏳** (mix task + RPC).
4. **Fase 4 — Auth externa plugável (P4).** Contrato de provider + mTLS-identidade como 1º provider.
5. **Fase 5 — Multi-tenancy / ACL por-recurso (P5).** O maior; habilita venda multi-tenant.

Cada fase é uma decisão própria (opções + recomendação) quando for implementada, seguindo a cadência do
`CLAUDE.md`.

---

## 6. Referências

- Práticas de segurança e testes: [SECURITY_DEVELOPMENT.md](SECURITY_DEVELOPMENT.md)
- Design do port NorthGuard: [NORTHGUARD_PORT.md](NORTHGUARD_PORT.md)
- Código: `lib/malachi/auth.ex`, `lib/malachi/auth/{user_store,session_manager,lockout_manager,config_validator}.ex`,
  `config/config.exs`, `config/runtime.exs`, `lib/malachi/tcp_protocol.ex`, `lib/malachi/dashboard.ex`.
