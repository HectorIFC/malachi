# Malachi → NorthGuard: design doc de port open-source em Elixir

> Status: **proposta** · Estratégia escolhida: **B faseado** (Elixir puro → Rust NIF só onde o profiling exigir)
> Decisão de viabilidade registrada em [Viabilidade](#1-viabilidade-medida) (benchmark em `benchmark/storage_viability.exs`).

Objetivo: reimplementar a **arquitetura** do [NorthGuard](https://www.linkedin.com/blog/engineering/infrastructure/introducing-northguard-and-xinfra)
(log storage escalável da LinkedIn) como projeto **open source em Elixir**, partindo do
malachi atual (broker TCP 100% in-memory).

> **Objetivo evoluído (a partir de 2026-06-27):** com a arquitetura NorthGuard concluída e sem SPOF
> (control plane Raft multi-nó + data plane com replicação por quórum, self-healing, failover,
> catch-up e membership SWIM), o novo norte é tornar o **malachi um produto escalável e vendável,
> melhor que o Kafka open source**, usando os conceitos do NorthGuard. Hoje há **dois mundos
> desconectados**: o broker TCP in-memory vivo (modelo de filas/pub-sub, estilo RabbitMQ) e o stack
> NorthGuard (modelo de log, estilo Kafka). **A prioridade #1 é conectá-los** (expor o stack
> NorthGuard ao cliente — a "Fase 3" abaixo); escala (incl. sharding) deixa de ser fora do alvo.

---

## 1. Viabilidade (medida)

Benchmark de I/O do BEAM puro no caminho crítico do storage (Apple M1, OTP 28 JIT, SSD NVMe).
Reproduza com `elixir benchmark/storage_viability.exs`.

| Cenário (Elixir puro, `:file` raw) | Throughput | Latência/flush |
|---|---|---|
| Durável fsync/batch, 10MB (alvo NG) | 472 MB/s · 484k rec/s | p50 **8.0ms** · p99 92ms |
| Durável, 20k×256B (count-driven) | 1234 MB/s · 5M rec/s | p50 3.4ms · p99 12.7ms |
| Não-durável (teto) | 612–1507 MB/s | — |
| Leitura sequencial | 2487 MB/s | — |

**Alvo real do NorthGuard:** ~20 MB/s de escrita por broker (steady-state), ~60 MB/s com
replicação 3×. O Elixir puro entrega **~10–50× isso** num laptop. **Throughput não é o gargalo.**

**Veredito:** Fase 0/1 em Elixir puro é viável e performática. O Rust NIF (Fase 2) se justifica
por três motivos — e nenhum deles é throughput:
1. **Cauda de latência** (vistos picos de até ~1.5s no max sob flush grande; pode piorar sob
   concorrência real de milhares de conexões + N segments com fsync simultâneo).
2. **`O_DIRECT`** — NorthGuard usa Direct I/O para evitar degradação do page cache em réplicas
   não-consumidas e leituras de segments antigos. BEAM puro não expõe O_DIRECT.
3. **Cache em nível de aplicação** alimentado pelos consume streams (idem item 2).

Caveat: no macOS `:file.sync` não faz `F_FULLFSYNC`; latências são otimistas vs. Linux server
(onde fsync é mais lento porém mais consistente). Throughput é representativo.

---

## 2. Gap analysis: malachi hoje vs. NorthGuard

O malachi já tem **a metade que o BEAM faz melhor que C++**. O que falta é a metade durável.

| Capacidade | malachi hoje | NorthGuard | Gap |
|---|---|---|---|
| Data plane TCP, conexões, sessões | ✅ `tcp_acceptor(_pool)`, `tcp_protocol`, `connection_registry` | sessionized streaming | **Adaptar** (pipelining/windowing) |
| Backpressure / flow control | ✅ `backpressure`, `rate_limiter`, `connection_limiter` | windowing por stream | **Adaptar** |
| Fila / partição em memória | ✅ `queue`, `partition_manager`, `consumer`, `ack_manager` | range/segment durável | **Substituir modelo** |
| **Persistência durável (log)** | ❌ tudo em memória | fps-store (WAL, file-per-segment, fsync) | **Construir** ← núcleo |
| **Modelo record→segment→range→topic** | ❌ (queue/partition) | ✅ | **Construir** |
| **Metadados sharded (DS-RSM/Raft)** | ❌ | ✅ vnodes + coordinators | **Construir** |
| **Membership gossip (SWIM)** | ❌ (`connection_registry` local) | ✅ | **Construir** |
| **Striping / self-balancing** | ❌ | ✅ segment como unidade de replicação | **Construir** |
| Auth / TLS / métricas / dashboard | ✅ `auth/*`, `tls_validator`, `metrics`, `dashboard` | (interno LinkedIn) | **Manter** |
| Storage policies / attributes | ❌ | ✅ expressões sobre attributes | **Construir** |

---

## 3. Arquitetura-alvo

### 3.1 Modelo de dados (igual ao NorthGuard)

```
Topic   ── coleção nomeada de Ranges que cobrem todo o keyspace
 └ Range  ── abstração de LOG: segments p/ uma faixa contígua de chaves (active|sealed)
    └ Segment ── UNIDADE DE REPLICAÇÃO: sequência de records (active|sealed; sela em 1GB / 1h / falha)
       └ Record ── key + value + headers (bytes), offset lógico no segment
```

- **Split/merge de range são operações puramente lógicas de metadados** — segments nunca são
  fisicamente combinados/copiados (confirmado no vídeo do meetup). Merge só entre *buddy ranges*
  (estilo buddy allocator).
- Ordenação total preservada via happens-before em splits/merges.

### 3.2 Camada de storage — `Malachi.SegmentStore` (behaviour pluggable)

NorthGuard diz que o storage é pluggable ("fps-store" é só a impl primária). Replicamos isso:

```elixir
@callback open(seg_id, opts) :: {:ok, handle} | {:error, term}
@callback append(handle, batch :: [record]) :: {:ok, base_offset} | {:error, term}
@callback sync(handle) :: :ok | {:error, term}        # fsync — chamado antes do ack
@callback read(handle, offset, max_bytes) :: {:ok, [record]} | :eof
@callback seal(handle) :: :ok                          # torna imutável
@callback sparse_index(handle, offset) :: {:ok, file_pos}
```

- **Fase 0/1:** `Malachi.SegmentStore.Elixir` — `:file` `[:raw, :binary]`, batching por
  10ms / N records / N bytes, fsync antes do ack, índice esparso em ETS/DETS ou arquivo `.idx`.
- **Fase 2 (condicional):** `Malachi.SegmentStore.Native` — Rust NIF (Rustler) com O_DIRECT,
  buffers alinhados, cache em nível de app; índice em `erlang-rocksdb`.

### 3.3 Metadados — DS-RSM com `ra`

- **vnode** = grupo Raft (`ra`) guardando um shard do metadado (topics/ranges/segments).
- **coordinator** = líder do vnode; carrega a "lógica de negócio" (sela/deleta topic,
  split/merge range, replica sets de segment, self-healing de segments sub-replicados).
- **DS-RSM** = vnodes sobre um hash ring (consistent hashing). Hash por nome do topic; por
  range ID para range/segment. **Posição do vnode no anel é estável** mesmo quando réplicas
  Raft entram/saem (detalhe do vídeo). **vnodes podem dar split** (quebra o estado em dois
  grupos Raft).

### 3.4 Membership — SWIM

- `partisan` (suporta SWIM) ou impl própria. Random probing p/ detecção + dissemination
  estilo infecção. Espalha **estado global mínimo**: host/port/attributes dos brokers +
  fronteiras/líder/term dos vnodes (para roteamento).

### 3.5 Protocolos

- **Metadados = unary** (1 req → 1 resp): create/delete/topicMetadata/segmentMetadata.
  Qualquer broker é proxy → roteia ao líder do vnode via estado gossipado.
- **Dados = streaming sessionizado** (produce/consume/replication) com pipelining + windowing.
  Reaproveita `tcp_protocol` + `backpressure` do malachi. Consume pode usar `:file.sendfile`.

### 3.6 Storage/metadata policies

- Policy = nome + retenção + constraints. Constraint = expressão sobre **attributes**
  (k/v opacos que admins ligam a brokers). Generaliza rack/DC-awareness sem o core entender
  "rack". Decide replica sets de segment e replicas de vnode.

---

## 4. Roadmap faseado

### Fase 0 — Persistência e modelo de log (Elixir puro)
- ✅ `Malachi.Storage.SegmentStore` behaviour + impl `Malachi.Storage.ElixirStore`
  (file-per-segment, batching, fsync-antes-do-ack, índice esparso, sealing, crash recovery,
  `open_read` barato p/ segments selados via `.idx`).
- ✅ Tipos `Malachi.Log.Record` (framing + CRC32) e `Malachi.Log.Segment` (offsets lógicos).
- ✅ `Malachi.Log` — log multi-segment: rolling/sealing automático (size/age), leitura
  contínua atravessando segments, recovery do diretório inteiro (só escaneia o último segment).
- ✅ Flush por **tamanho** (`:flush_bytes`, default 10MB — gatilho de tamanho do NorthGuard):
  `append` faz flush+fsync automático ao atingir o limite.
- ✅ `Malachi.Range` — abstração de log sobre keyspace `[0, 2^bits)`: chaves por hash
  (`:erlang.phash2`), ranges como blocos buddy-allocator, **split/merge lógicos** (não movem
  dados), buddy único, linhagem (`parents`) para **happens-before**.
- ✅ `Malachi.Topic` — coleção nomeada de ranges que **cobrem todo o keyspace**: roteamento de
  records por chave (`route`/`append`), orquestração de split/merge mantendo a cobertura,
  sealing, ranges selados retidos para leitura. Metadados do topic são **em memória** por ora
  (persistência durável vem com o DS-RSM da Fase 1).
- ✅ Índice esparso em `:array` (busca binária O(log n), insert O(log n)).
- ✅ **Recovery em chunks** (`scan_segment`) — nunca carrega o segment inteiro na memória.
- ✅ **Flush por contagem** (`:flush_count`, default 20k) no `ElixirStore` + **flush por tempo**
  (~10ms) e **acesso concorrente serializado** no `Malachi.TopicServer` (GenServer que envolve o
  Topic, com timer de flush e flush no shutdown).
- ✅ **Leitura cross-epoch** (`Topic.read_history/2`) — histórico de uma chave através de um split
  (epoch do pai → epoch do filho), via linhagem + happens-before.
- ✅ Testes: property-based (`stream_data`) + unit — append/read/seal/crash-recovery/roll/
  auto-flush (size+count) + split/merge/buddy/happens-before + cobertura/roteamento/cross-epoch +
  flush por tempo e concorrência no TopicServer. **66 testes + 2 propriedades.**

> **Fase 0 concluída.** Único item adiado de propósito: **persistência durável dos metadados do
> topic** (quais ranges existem) — entra no **DS-RSM da Fase 1**, fiel ao NorthGuard, onde essa
> metadata vive no coordinator/vnode (Raft).

### Fase 1 — Distribuição

Estratégia confirmada: **lógica pura primeiro, `ra` depois** (mesmo padrão de
`ElixirStore`→Rust NIF). A máquina de estado do metadado é desenhada com o contrato
`apply/2` de um Raft, então o `ra` pluga sem retrabalho.

**Fase 1a — DS-RSM (lógica pura, sem deps):**
- ✅ `Malachi.Cluster.HashRing` — consistent hashing: vnodes em tokens no anel `[0, 2^bits)`,
  `route` por ceiling com wraparound, `boundaries` (arco do vnode), add/remove. Movimentação
  mínima ao adicionar/remover vnode (testado).
- ✅ `Malachi.Keyspace` — math de keyspace compartilhada (`size_for_bits!`, `position_of`,
  `within?`, `splittable?`, `split_point`, `buddies?`); `Range` e `HashRing` refatorados para
  usá-la (mata a duplicação de hash + validação `1..32`).
- ✅ `Malachi.Metadata` — máquina de estado do vnode/coordinator (`apply/2` **determinístico**,
  contrato de Raft): topics, ranges (split/merge buddy via `Keyspace`, linhagem) e segments
  (replica set, estado active/sealed). Ids de range vêm de um contador no estado (sem
  `unique_integer`/timestamp dentro do `apply` → seguro p/ replicação). **Resolve a persistência
  de metadados do topic adiada da Fase 0.** Testado inclusive com replay do log (determinismo) e
  com catch-all defensivo (comando desconhecido retorna erro, não derruba a réplica).
  - ⏳ Futuro: índices secundários `%{topic => range_ids}` e `%{range_id => segment_ids}` — hoje
    `seal_topic`/`delete_topic`/`ranges_of_topic`/`segments_of_range` varrem todos os ranges/
    segments (O(n)); aceitável por shard, otimizar quando um vnode acumular muito metadado.
- ✅ `Malachi.Cluster.DSRSM` — junta tudo: HashRing + um `Metadata` por vnode; `command/3` e
  queries roteados por **nome do topic** ao vnode dono (sharding de topics entre vnodes, testado).
  Determinístico (replay). Decisão Fase 1a: metadado de um topic **co-localizado** num vnode
  (route por nome) — desvio anotado do range-id sharding do NorthGuard.
- ✅ **Split de vnode** (o "D" — dinâmico — do DS-RSM): `DSRSM.split_vnode/3` adiciona um vnode
  e **migra** os topics deslocados (topic + ranges + segments) para ele. Viabilizado por range id
  `{topic, seq}` (globalmente único → sem colisão na migração); helpers `Metadata.extract_topic/2`
  e `insert_topic/2`. Testado: migração sem perda + ranges/segments acompanham o topic.
  - ⏳ Futuro: sharding de range/segment por range id (cross-vnode), que é o desvio restante
    do NorthGuard.

**Bridge control plane → data plane (1a.5):**
- ✅ `Malachi.Broker` — compõe o control plane (`Metadata`, fonte da verdade da estrutura) com
  o data plane (um `Log` por range, indexado por range id). `produce` roteia por chave usando os
  ranges ativos do `Metadata` + `Keyspace`; `split_range`/`merge_ranges` passam pelo `Metadata`
  (estrutura) e selam/flusham os logs afetados. As decisões lógicas vivem só no control plane.
- ✅ Leitura **cross-epoch** migrada para o `Broker` (`read_history`/`stream_history`, linhagem via
  `Metadata.parents`) + `Broker.pending?`. Validação de **nome de topic** no `Metadata.create_topic`
  (allowlist; rejeita `..`/`/`) — fecha o path-traversal no data plane, no control plane.
- ✅ `Malachi.BrokerServer` — GenServer sobre o `Broker`: flush por **tempo** (~10ms) + acesso
  **serializado** (concorrência), flush no shutdown. Paridade com o antigo `TopicServer`.
- ✅ **Duplicação removida:** `Topic`/`TopicServer`/`Range` **deletados** (sua orquestração de
  split/merge/coverage/lineage duplicava o `Metadata`). O caminho único agora é
  control plane (`Metadata`/`DSRSM`) + `Broker`/`BrokerServer`. **Bridge concluído.**

**Hardening do DS-RSM (property-based, substituto da simulação determinística):**
- ✅ Property tests model-based (`stream_data`) para `Metadata` e `DSRSM`: sequências aleatórias
  de create/split/merge/register/seal/delete + **split de vnode**, sempre escolhendo alvos
  válidos do estado atual. Invariantes verificadas: cobertura do keyspace por topic ativo,
  integridade referencial (range→topic, segment→range, id `{topic, seq}` bem-formado), **nenhum
  topic órfão** (sempre vive no vnode que o roteia, mesmo após split de vnode) e **determinismo**
  (mesma sequência → mesmo estado).

**Fase 1b — Replicação e membership:**
- ✅ **`ra` integrado** (Raft real): `Malachi.Cluster.MetadataMachine` é uma `:ra_machine` cujo
  `apply/3` **delega ao `Metadata.apply/2`** — a lógica de negócio não muda, só ganha replicação.
  `Malachi.Cluster.MetadataServer` é o wrapper fino (start de cluster single-node, `command` via
  log Raft, `query` linearizável, restart). Um cluster ra = um vnode; liderança = coordinator.
  Testado: comandos replicados, query consistente, erro de comando propagado, **estado sobrevive
  a restart** (log durável). Determinismo do `Metadata` (property tests) é o que torna isso seguro.
- ✅ **`Malachi.Cluster.ReplicatedDSRSM`** — o DS-RSM sobre Raft: HashRing + **um cluster `ra` por
  vnode**. `command`/`query` roteiam por nome do topic ao cluster do vnode dono (sharding +
  replicação por shard). Contraparte de produção do `DSRSM` puro (que os property tests exercitam).
  Testado: roteamento ao cluster certo, query consistente, **sharding entre 2 clusters Raft reais**
  (cada topic só existe no cluster que o roteia), erro de comando propagado, ring vazio → `:no_vnode`.
  Escopo: **vnodes estáticos** (split-sobre-ra adiado), single-node (multi-node depende de membership).
  - ⏳ Próximo: split de vnode sobre `ra` (migrar metadado entre grupos Raft); cluster multi-node.
- ✅ **`Malachi.Cluster.Placement`** — política **pura** de placement + self-healing de réplicas
  de segment (a camada de *decisão* do data plane; o `Metadata` já guarda o *estado* dos segments).
  Usa **rendezvous (HRW) hashing**: `place/3` escolhe o replica set (determinístico → seguro p/
  Raft; mínimo reshuffle), `under_replicated/3` detecta segments com réplica morta (alvo clampado
  ao nº de brokers vivos), `heal/3` emite comandos `:set_segment_replicas` que restauram a
  replicação. Cobre segments **sealed** também (durabilidade). `available_brokers` é parâmetro
  abstrato — fixa o contrato que a futura membership servirá. Property tests: tamanho/distinção do
  replica set, determinismo independente de ordem, **retenção sob remoção de broker (min-reshuffle)**
  e **fixpoint do `heal` em uma passada**.
- ✅ **Ciclo de vida do segment no `Broker`** — o data plane agora *cria* segments. A cada range
  ativo o `Broker` mantém um segment aberto (span lógico de offsets sobre o `Log` único do range,
  A1): no 1º produce registra o segment escolhendo o `replica_set` via `Placement.place` sobre
  `:brokers`/`:replication_factor` (defaults `[node()]`/`1`); contabiliza os bytes apendados
  (`Record.encoded_size/1`, casado byte-a-byte com `encode/1`) e **sela + rola** ao cruzar
  `:segment_max_bytes` (threshold *soft*, checado no limite do batch). `split`/`merge` selam o
  segment ativo do range afetado. `segment_id = {range_id, seq}` (globalmente único; seq por-range
  persiste entre selas → id nunca reusado). API de `produce`/`read` inalterada (segments são
  bookkeeping aditivo). Testado: registro no 1º produce, replica set via HRW, rollover por bytes
  (1 record/segment e overshoot soft), sela em split/merge, validação de policy.
  - ⏳ Próximo: fiar `heal` (re-replicação) num gatilho de mudança de membership; rollover por
    tempo/contagem além de bytes.
- ✅ **`Malachi.Cluster.ReplicaTracker`** — núcleo **puro** de commit por quórum da replicação de
  **um segment** (a lógica determinística do mecanismo, sem processos/rede). `replica_set` ordenado
  (1º = primário); `ack/3` registra o offset durável de cada réplica (monotônico, ignora regressão);
  `commit_offset/1` = maior offset presente em **maioria** (⌊N/2⌋+1) das réplicas, ou `:none`;
  `committed?/2`. Tolera ⌊(N-1)/2⌋ falhas sem esperar a réplica mais lenta. Contraparte (para *dados*
  de segment) do `Metadata` determinístico (para *metadata*). Property tests: commit = maior offset
  em quórum, **monotonicidade do commit**, `committed?` sse quórum o tem. Escopo: replica set fixo
  (failover de primário/mudança de set depois).
- ✅ **`Malachi.Cluster.ReplicationServer`** — o **transporte** da replicação: um `GenServer` por
  broker que envia o stream do segment ativo do **primário** aos **seguidores** e dá ack quando um
  **quórum** armazenou de forma durável. Broker = referência do processo (nome local nos testes;
  `{nome, node}` entre nós — `GenServer.call` aceita ambos, então o mesmo caminho roda in-process e
  multi-node). `replicate/4` no primário apenda+fsync local, faz fan-out concorrente aos seguidores,
  alimenta o `ReplicaTracker` e retorna `{:ok, last}` quando o quórum tem o batch (tolera ⌊(N-1)/2⌋
  seguidores lentos/caídos) ou `{:error, :no_quorum}`. Primário e seguidores fazem `fsync` antes de
  contar para o quórum → "committed" = durável na maioria. Cada segment abre seu `Log` em
  `base_offset = start_offset` (carregado no `replicate/5`), então os offsets dos segments de um
  range são **contíguos** (não reiniciam em 0 por segment). Testado in-process: replica a todos +
  commit, tolerância a 1 seguidor caído, `:no_quorum` sem maioria, single-replica, offsets contíguos
  entre batches, **base não-zero** (`:out_of_range` abaixo da base), `:not_primary`, batch vazio,
  replica set vazio (sem crash), set duplicado (sem deadlock). Escopo: caminho feliz do segment ativo.
- ✅ **`ReplicationServer` fiado no `Broker`/`BrokerServer` (A2+A3)** — o `Broker` virou
  **roteador puro** (control plane): perdeu `logs`/`directory`, mantém só `Metadata` + ciclo de
  vida do segment + contador de offset por range. O storage é 100% do `ReplicationServer`. A
  ligação usa **funções de efeito injetadas**: `produce` recebe `replicate_fun`, `read`/
  `stream_history` recebem `read_fun` — toda a orquestração (rota, offset→segment, commit, paginação
  e filtro do cross-epoch) fica num só módulo, testado com **fakes in-memory**
  (`Malachi.Test.FakeSegmentStore`). O `BrokerServer` injeta `&ReplicationServer.replicate/5` e
  `&ReplicationServer.read/4`, inicia um `ReplicationServer` local (ref = pid) e abre o `Broker` com
  `brokers: [esse_ref]`. Como a escrita é fsync-por-quórum no retorno, o **flush por tempo (10ms)
  saiu** e `sync/1` virou no-op. `produce` retorna `{broker, {:ok, placements} | {:error, reason}}`
  (commit por grupo; valor imutável dá transação grátis). Sem duplicação de storage. Suíte completa
  verde (831 testes).
- ✅ **`Malachi.Cluster.Catchup` (primitiva de catch-up/backfill)** — copia os records de um
  segment de uma réplica **fonte** para uma **alvo** no intervalo `[from, to)`. Serve aos dois
  casos: seguidor atrasado fechando o gap do segment ativo, e réplica nova (do `Placement.heal`)
  fazendo **backfill** de um segment selado inteiro. Roda por **orquestração externa** (no processo
  do chamador, via `ReplicationServer.read/4` na fonte + `follow/4` no alvo) — nada aninhado num
  `handle_call` que está sendo aguardado, então não há o deadlock primário↔seguidor. Expostos no
  `ReplicationServer`: `follow/4` (append de réplica) e `end_offset/2` (até onde o alvo está, ou
  `:empty`). Se a fonte estiver ela mesma atrás, para no que tem e devolve o offset alcançado.
  Testado in-process: backfill de réplica fresca, catch-up só do gap, base não-zero preservada,
  fonte incompleta, no-op quando já alcançado, `:empty`. Escopo: a primitiva (dirigida pelo
  chamador).
- ✅ **`Malachi.Cluster.SelfHealing` (heal → backfill de selados)** — fecha o loop de self-healing
  para segments **selados**: `Placement` decide, `Catchup` executa. `heal_sealed/4` (metadata +
  brokers vivos + rf) acha os selados sub-replicados, escolhe o replica set curado via
  `Placement.place`, faz **backfill** de cada réplica nova a partir de uma réplica viva
  (`Catchup.run`), e devolve os comandos `:set_segment_replicas` (que tiveram backfill ok) para o
  control plane aplicar, além dos segments que não deu para curar (ex.: `:no_live_source`). Só
  selados (range `[start, start+length)` fixo → backfill bem-definido); o segment ativo se recupera
  pelo gatilho no write-path (fatia (a), adiada). Brokers vivos vêm como parâmetro (membership
  depois). Testado in-process: backfill da réplica nova + comando + loop fecha (re-heal vazio),
  todas as réplicas mortas → `:no_live_source`, nada a fazer quando ok, ativo ignorado.
- ✅ **Catch-up automático no write-path (segment ativo)** — fecha a metade que faltava da
  recuperação (a de selados é o `SelfHealing`). Quando o fan-out do primário chega num seguidor com
  `next_offset < expected_first` (ficou para trás), o `ReplicationServer` **dispara um processo
  monitorado em background** que roda o `Catchup` puxando do primário (`from = seu próprio fim`,
  `to = fim do primário`), responde `:out_of_sync` (a escrita commita via as réplicas em dia) e o
  seguidor **reentra no quórum num batch posterior**. Um conjunto `catching_up` deduplica catch-ups
  por segment; o `:DOWN` do monitor limpa o flag em sucesso/falha; a checagem de offset do `follow`
  garante segurança sob corrida (um append concorrente faz o catch-up abortar e o próximo gap
  re-disparar — converge, sem duplicar/corromper). A mensagem de `follow` carrega o ref do primário
  como fonte; os `follow` do `Catchup` passam `nil` (não re-disparam). Testado in-process:
  seguidor perde um batch → é caçado de volta ao log completo → reentra no quórum.
- ✅ **Backfill de réplica nova em segment ativo (missed-start)** — extensão mínima do gatilho: a
  mensagem de `follow` passa a carregar o `base` do segment e o log fresco abre **no `base`** (não
  no offset do batch). Assim uma réplica **recém-adicionada** a um segment ativo vê o gap do começo
  (`next = base < expected`), o gatilho existente puxa `[base, cabeça)` e ela **converge na cabeça
  móvel** via os re-disparos missed-middle, entrando no write-path ao alcançar (estilo ISR: não conta
  no quórum até sincronizar). Zero código de gatilho novo. Limitação conhecida: sob escrita sustentada
  muito rápida pode não alcançar (sem throttling — refinamento futuro). Testado in-process: réplica
  nova num segment ativo backfilla do começo e passa a seguir ao vivo.
  - ⏳ Próximo: failover de primário; multi-node (membership/SWIM).
- ✅ **`Malachi.Cluster.Membership` (máquina de estado SWIM pura)** — a view determinística de
  membership: `alive`/`suspect`/`dead` por membro + **incarnation**, sem processos/timers/rede. A
  regra única é um *join* na ordem lexicográfica `{incarnation, rank}` (`alive < suspect < dead`),
  que dá a **precedência SWIM** (incarnation maior vence; empate → suspect>alive, dead>ambos; igual
  → idempotente) e garante **convergência** do merge de gossip em qualquer ordem (CRDT-like).
  Exceção: update sobre **self** suspeito/morto é **refutado** subindo a própria incarnation e
  re-anunciando alive (efeito `{:refute, …}` para disseminar). `apply_update/2`, `merge/2`,
  `suspect/2`, `confirm/2`, `alive_members/1`. Property tests: **convergência independente de ordem**
  e **monotonicidade de incarnation**; + unidades de precedência e self-refute.
- ✅ **`Malachi.Cluster.MembershipServer` (servidor SWIM: detector + gossip)** — `GenServer` por
  broker que roda a `Membership` ao vivo. A cada *protocol period* faz **ping** num peer vivo
  aleatório; sem **ack** no `ack_timeout` → `suspect`; sem refute no `suspicion_timeout` → `dead`.
  Cada ping/ack faz **piggyback** da view (lista de updates) → anti-entropy → as views **convergem**;
  um nó falsamente suspeito descobre pelo ack ao seu próprio ping e refuta (sobe incarnation). Refs
  location-transparent (testável in-process; multi-node sem mudança); envios fire-and-forget (peer
  morto = sem ack = detecção). Peers semente via `:peers`; timers configuráveis. Testado in-process:
  **gossip espalha** conhecimento parcial até todos conhecerem todos, **nó parado é detectado e vira
  `dead`** no cluster, e **refute** de suspeita falsa sobre si.
- ✅ **Ping indireto** — quando o ack direto não chega, o nó pede a `indirect_fanout` peers (default 3)
  que **pinguem o alvo por ele** (`ping_req` → relay proba o alvo → relaia o ack ao requester); só
  suspeita se nem o direto nem o indireto responderem no `indirect_timeout`. Reduz falsos positivos sob
  perda transitória da rota direta. Testado: a **cadeia de relay** entrega o ack do alvo ao requester
  (determinístico); o teste de nó morto agora exercita o caminho completo (direto→indireto→suspect→dead).
- ✅ **Join formal** — ao iniciar, o nó manda `{:join}` a cada seed (`:peers`); o seed **registra o
  joiner vivo** e responde com sua **view completa**, então o joiner conhece o cluster de uma vez (em
  vez de só convergir por gossip). Best-effort (gossip é a rede de segurança). Testado: handshake
  (seed responde `join_ok` com a view e passa a conhecer o joiner) e **nó novo aprende o cluster
  inteiro via um único seed**. Ressalva: rejoin-após-morte (reinício com incarnation 0 < `dead@n`)
  fica para depois. **SWIM fechado:** ping direto+indireto + suspicion + gossip + join.
- ✅ **`Malachi.Cluster.HealCoordinator` (self-healing reativo)** — `GenServer` periódico que fecha
  o loop **broker morre → detectado → curado**: a cada intervalo roda `SelfHealing.heal_sealed` com
  o conjunto **vivo** e aplica os comandos. Desacoplado por **seams injetados** (`live_brokers`,
  `metadata_source`, `apply_command`, `rf`, `interval`) — testável in-process e fiável depois ao
  `MembershipServer` (live) e ao control plane (apply) sem mudança. `heal_now/1` roda uma passada
  síncrona (testes/trigger manual). Testado in-process: cura sob réplica perdida (backfill +
  comando aplicado + loop fecha), `:no_live_source`, e **cura automática no timer**.
- ✅ **Fiação fina 1a — membership → self-healing reativo (controle único + N data-brokers)** — o
  loop reativo agora roda ponta a ponta na topologia **1 nó de controle + N data-brokers**: o
  `BrokerServer` aceita `:brokers` externos (refs de `ReplicationServer`) e é a autoridade do
  metadata; ganhou `metadata/1` e `apply_heal/2` (que aplica `set_segment_replicas` via novo
  `Broker.apply_heal/2`). O `HealCoordinator` é fiado com `metadata_source`/`apply` ← `BrokerServer`
  e `live_brokers` ← **bridge** `MembershipServer.alive_members |> mapa(member-id → broker-ref)`.
  Testado: **data-broker morre → membership detecta → segments selados re-replicados** ao conjunto
  vivo (sem sub-replicação restante, dados conferidos nas novas réplicas), e a bridge derruba a
  broker-ref do nó morto.
- ✅ **Placement dinâmico** — o `BrokerServer` agora **refresca** periodicamente os brokers de
  placement do `Broker` a partir de `:live_brokers` (`Broker.set_brokers/2`), então um **novo segment
  nasce no conjunto vivo** em vez de nos configurados (fallback: mantém o último não-vazio; sem custo
  no hot path). Completa a simetria do reativo (cura **e** criação reagem ao vivo). Robustez junto:
  `ReplicationServer.replicate/5` agora **não derruba** o chamador se o primário estiver morto
  (retorna `{:error, :unreachable}` → `produce` propaga erro), e um `produce` falho **descarta** o
  segment recém-aberto (rollback grátis via valor imutável — sem segment fantasma; o retry re-placeia
  nos vivos). Testado: após um data-broker morrer e sair do conjunto vivo, um segment recém-produzido
  **exclui o morto** do `replica_set`.
- ✅ **Failover de primário** — `Malachi.Cluster.Failover.plan/2` (puro): para cada segment **ativo**
  cujo primário (`hd(replica_set)`) está morto, emite `set_segment_replicas` **reordenado** com uma
  réplica viva no topo (a morta fica como seguidor que não dá ack; o `heal` a troca após o seal —
  sem mover dado). O `Broker.apply_heal` foi generalizado para atualizar também o **cache do segment
  ativo** (um só caminho de apply serve heal e failover), e o `HealCoordinator` virou um loop de
  **reconciliação** (heal selados + failover ativos a cada tick). Testado: `plan` puro (promove vivo,
  ignora selado, pula tudo-morto), `apply_heal` atualiza cache+metadata, e **integração**: primário
  de segment ativo morre → promovido → escrita continua no novo primário (ambos os records lidos).
- 🚧 **1b — autoridade do metadata via `ra` (em fatias).**
  - ✅ **`Malachi.Cluster.ReplicatedMetadata`** (componente) — pareia um `MetadataServer` (cluster `ra`
    rodando `Metadata.apply`) com um **cache `Metadata` local**: `command/2` vai ao log Raft e, no
    commit, aplica o **mesmo** comando no cache (determinismo → cache == estado replicado), então as
    leituras são locais (sem round-trip Raft no hot path) com **read-your-writes**. `metadata/1` (leitura),
    `refresh/1` (re-lê o estado replicado p/ multi-writer/recovery), `start/1`, `delete/1`. Testado:
    comando commitado atualiza o cache, comando rejeitado deixa o cache intacto + erro da máquina,
    cache == estado replicado (refresh no-op p/ escritor único). Cluster `ra` single-node (durabilidade;
    HA multi-nó depois).
  - ✅ **`BrokerServer` fiado no metadata via `ra`** — o `Broker` ganhou um **`command_fun` injetado**
    `(metadata, command) -> {metadata, reply}` (default `&Metadata.apply/2` → comportamento in-memory
    intacto); **todas** as mutações (`create_topic`/`split`/`merge`/`register`/`seal`/
    `set_segment_replicas`) passam por ele, leituras seguem no `broker.metadata` (cache). Com a opção
    `:metadata_cluster`, o `BrokerServer` inicia um `MetadataServer`, **semeia** o cache do estado
    replicado, e injeta `command_fun = ReplicatedMetadata.apply_command(server_id, …)` — comando Raft +
    apply no cache **threadado pelo `broker.metadata`** (um produce pode abrir+selar segment com
    read-your-writes intra-operação). Testado (ra): mutações (topic + segment) ficam no **estado
    replicado** (query direto no `ra`) e o cache == replicado; comando rejeitado propaga o erro da
    máquina. Default (sem `:metadata_cluster`) segue in-memory.
  - ✅ **Hardening do crash-path do `register`** — `open_segment` agora retorna `{:ok, broker}` |
    `{:error, reason}`; uma falha do `register_segment` (ex.: timeout do `ra`) é propagada pelo
    `ensure_segment` → `produce` como `{:error, reason}` (rollback do open — sem segment fantasma),
    em vez de derrubar o `BrokerServer`. Testado: `command_fun` que falha o register → produce
    `{:error, :ra_down}`, nenhum segment registrado, offset não avançado.
  - ✅ **Cluster `ra` multi-nó (HA do control plane — último SPOF eliminado)** — `MetadataServer.start/2`
    forma o cluster `ra` em **vários nós** (`server_ids` entre nós); `BrokerServer` aceita
    `:metadata_nodes`. Com ≥3 membros o metadata é replicado e **sobrevive à queda de um nó de
    controle** (um seguidor é eleito líder). Testado de verdade (`test/.../metadata_ha_test.exs`, tag
    `:multinode`, opt-in via `mix test --include multinode`): 3 nós BEAM reais via `:peer`, comando
    replicado a todos, **kill abrupto do nó-líder**, e um comando seguinte ainda comita (novo líder) com
    o metadata anterior intacto — flake-checado 5×. (Excluído do `mix test` padrão por exigir
    epmd/distribuição.) **1b concluído.**

### Fase 3 — Produto: conectar os dois mundos + escala (novo norte)
Tornar o stack NorthGuard o broker **vivo** e escalável, melhor que o Kafka OSS.

- 🚧 **B — Conectar TCP/cliente ↔ stack NorthGuard (log API com cursor opaco).** Hoje o `tcp_protocol`
  fala filas (`publish`/`subscribe`/`ack`/`channel_*`) sobre `Queue`/`Channel`; o stack NorthGuard fala
  log sobre `BrokerServer`. **Contrato de cliente = jeito NorthGuard, NÃO Kafka:** o cliente usa
  `topic` + **chave** (produce) + **cursor opaco** (consume) — **nunca** vê partition/offset (escondidos
  de propósito, para o sistema split/merge/restripe por baixo sem quebrar o cliente; é o diferencial vs
  Kafka). O cursor é um token que hoje codifica `%{range_id => offset}` por dentro, mas o cliente o trata
  como opaco. **Prioridade #1.** Fatias:
  - ✅ **1ª fatia — núcleo `Malachi.LogApi`:** `create_topic`/`produce` (por chave)/`fetch` (cursor
    opaco) sobre `BrokerServer`; decode do cursor **seguro** (`binary_to_term [:safe]` + validação de
    forma). Cliente nunca vê partition/offset. Testado isolado.
  - ✅ **1ª fatia — fiação:** `BrokerServer` (`Malachi.LogBroker`) subido na árvore de supervisão
    (`application.ex`, single-node/in-memory por default; data dir via `:log_data_dir`); ações
    `create_topic`/`produce`/`fetch` adicionadas ao `tcp_protocol` (aditivo — filas/canais continuam),
    com auth (`:produce`/`:consume`), `max` limitado, records→JSON **sem offset**. **Cliente alcança o
    stack NorthGuard de ponta a ponta** — testado e2e via TCP real (produce por chave → fetch por
    cursor opaco; permissões; re-fetch vazio).
  - ⏳ Próximas: consumo multi-range com split (cursor cobrindo ranges que dividem), consumer groups +
    posição commitada server-side, long-poll, deploy multi-nó/replicado, payloads binários (base64).
- ⏳ **C — Features NorthGuard restantes:** storage/metadata policies, attributes, **retenção**
  (time/size), compaction. Médio valor; não muda a usabilidade, mas é esperado de um produto.
- ⏳ **D — Sharding via `ReplicatedDSRSM`** (agora **no alvo**): metadata sharded (um cluster `ra`
  por vnode) para **escalar o control plane** além de um cluster Raft único. Refactor (o cache único
  do `BrokerServer` vira por-vnode/roteado por topic).

### Fase 2 — Eficiência nativa (condicional, guiada por profiling)
- `Malachi.SegmentStore.Native` em Rust (Rustler): O_DIRECT, cache de app, `erlang-rocksdb`.
- Só implementar se a Fase 1 mostrar cauda de latência/page-cache como gargalo sob concorrência.

### (Futuro) Camada Xinfra-like
- Virtual topics com epochs, offsets opacos, migração dual-write, consumer-group management.
  Fora do escopo inicial.

---

## 5. Dependências candidatas

| Necessidade | Lib | Notas |
|---|---|---|
| Raft (DS-RSM/vnodes) | [`ra`](https://github.com/rabbitmq/ra) | Raft de produção do RabbitMQ |
| Membership SWIM/gossip | [`partisan`](https://github.com/lasp-lang/partisan) | ou impl própria |
| Índice esparso nativo (Fase 2) | [`erlang-rocksdb`](https://github.com/emqx/erlang-rocksdb) | binding RocksDB |
| NIF nativo (Fase 2) | [`rustler`](https://github.com/rusterlium/rustler) | NIFs Rust memory-safe + dirty schedulers |
| Testes de propriedade | `stream_data` / `PropEr` | substituto parcial da sim determinística |

---

## 6. O que NÃO vamos replicar (e o substituto)

**Simulação determinística** (cluster+clientes single-thread, swap de time/net/disk/RNG, replay
exato de falhas) — um dos pilares de confiabilidade do NorthGuard — **é essencialmente inviável
no BEAM**, porque não controlamos o scheduler (preemptivo, multicore). É um downgrade real de
garantias e precisa ser aceito explicitamente.

Substitutos:
- Property-based stateful testing (`stream_data`/`PropEr`) do modelo de log e da máquina de estados.
- `Concuerror` para checagem de concorrência (escala limitada).
- Testes estilo Jepsen para consistência distribuída.
- Fault injection via `partisan` (partições de rede) + chaos no storage (corrupção/erro de I/O).

---

## 7. Questões em aberto

1. Replicação na Fase 1: implementar do zero sobre `ra`/streams, ou apostar mais no `ra`?
2. Índice esparso na Fase 0: arquivo `.idx` próprio vs. ETS persistido vs. DETS?
3. Compatibilidade de protocolo: manter o protocolo TCP atual do malachi ou desenhar o
   sessionizado do NorthGuard desde a Fase 0?
4. Formato de offset: opaco (estilo Xinfra) desde já, ou `long` simples no início?
