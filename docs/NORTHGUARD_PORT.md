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
  - ✅ **Consumer groups + posição commitada server-side (Opção A — offsets no Metadata RSM).** Um grupo
    consome com `fetch_group(topic, group)` (retoma da posição **duravelmente** commitada do grupo) e
    avança-a com `commit(topic, group, cursor)` (estilo at-least-once / commit manual do Kafka). O cursor
    segue **opaco** (o cliente nunca vê offset). Os offsets vivem no `Metadata` determinístico
    (`{:commit_offset, group, topic, offsets}` + query `committed_offsets/3`), então ganham durabilidade
    e HA **de graça** pelo caminho `command_fun`/`ra` existente. Exposto no `BrokerServer`/`LogApi` e no
    `tcp_protocol` (`fetch` aceita `"group"`; nova ação `commit`). Testado: unit do Metadata (last-commit-wins,
    grupos/topics independentes), round-trip `LogApi` (retoma após commit; re-leitura sem commit = at-least-once),
    e2e via TCP (produce → fetch por grupo → commit → fetch retoma vazio). Offsets commitados crescem o
    estado do Metadata (escala → futuro store log-based, estilo `__consumer_offsets`, alinhado ao roadmap D).
  - ✅ **Consumo split-aware (cursor cobrindo ranges que dividem).** Antes, o consumo lia só o offset
    linear de cada range **ativo**, então registros escritos **antes** de um split (que vivem nos segments
    do range-pai, agora selado e fora do conjunto ativo) eram **perdidos**. Agora `read_consume/5` no
    `Broker` faz leitura **cross-epoch ao vivo**: drena os ancestrais selados (filtrados à fatia de
    keyspace do range, via `parents` + `Keyspace`) e depois faz **tailing** do range ativo — sem marcar
    `:done` no self (diferente de `stream_history/5`, que é para história *bounded*), reusando
    `history_sources`/`filter_records` (DRY). A posição por range vira um `consume_cursor`
    (`:start | {source_index, source_offset}`), carregada opacamente no cursor do cliente e nos offsets
    commitados (tipo `Metadata.position`). `fetch`/`fetch_group` herdam o fix. **Split durante o consumo
    (decisão A):** os filhos não herdam a posição do pai → recomeçam do início e **reprocessam** sua fatia
    (at-least-once, zero perda; splits são raros/administrativos). Testado: `Broker` (entrega cross-epoch
    exata pós-split, tailing de novos registros, range inexistente) e integração `LogApi`/`BrokerServer`
    (fresh consumer vê todos os pré+pós-split; grupo commitado reprocessa após split).
  - ✅ **Payloads binários (base64 no protocolo JSON).** O storage e o `LogApi` sempre aceitaram bytes
    arbitrários (`build_record` é `is_binary(value)`); o gargalo era só a borda JSON: o `value` vinha do
    `Jason.decode` (sempre UTF-8) e, no fetch, um `value` não-UTF-8 **derrubava o `Jason.encode!`**.
    Agora **todo `value` no wire é base64** (escolha do esquema: uniforme): o `produce` decodifica
    (`Base.decode64`) cada `value` para bytes antes de chamar o `LogApi`, e o `record_to_json` codifica
    (`Base.encode64`) no fetch. base64 vive **só no `tcp_protocol`** (o `LogApi` segue binário-nativo);
    `key`/`headers` permanecem UTF-8. base64 inválido → `:invalid_base64`. **Breaking** (clientes JSON
    agora mandam/recebem `value` em base64). Testado e2e: round-trip de bytes não-UTF-8, base64 inválido
    rejeitado, e os testes existentes migrados para base64.
  - ✅ **Long-poll no `fetch`/`fetch_group` (event-driven, *waiters* no `BrokerServer`).** Antes, um
    consumidor *caught up* recebia `[]` na hora e tinha que re-pollar (busy-poll). Agora um `wait_ms`
    opcional (clampado a 30s no `tcp_protocol`) faz a chamada **bloquear** até um produce ao topic
    entregar dados ou o timeout expirar (`[]`). Mecanismo escolhido após **benchmark** (`bench/`): A —
    *waiters* dentro do `BrokerServer` (o GenServer que já serializa produce+fetch) guardam os fetches
    pendentes vazios (`{from, topic, positions, max}` + timer); o `produce` ao topic re-consome cada
    waiter e responde (`GenServer.reply`) os que têm dados, o timeout responde `[]`. Medições: A entrega
    ~35-47% mais rápido que pub/sub via Registry (B) em todas as escalas e é **self-contained** (sem
    estado global); o contra de A (produtor bloqueia no fan-out) só é material com milhares de
    consumidores no mesmo topic — fora do single-node atual; ao ir multi-nó, o fan-out migra para um
    mecanismo distribuído. A orquestração de leitura multi-range **migrou** do `LogApi` para o
    `BrokerServer` (`consume_ranges`): 1 call coesa em vez de N+1, e é o que o produce re-executa para
    acordar waiters. Testado: `BrokerServer` (acorda no produce, timeout vazio, acorda só o topic
    produzido), `LogApi` (bloqueia até produce; timeout), e2e TCP (wake por produtor concorrente; timeout).
  - 🚧 **Rearquitetura da camada de cliente (protocolo binário + streaming), guiada por benchmark.** A
    funcionalidade de log sobre TCP estava completa, mas o **estilo** era JSON+base64 request/response —
    não o "sessionized streaming com windowing" do NorthGuard. Decidido **empiricamente** (`bench/`,
    1M msgs): (a) **protocolo binário** vs JSON+base64 — **-29% bytes on-wire, 8.9x menos CPU no encode,
    17.3x no decode** (`protocol_bench.exs`); (b) **entrega push** (subscribers no broker) 35-44% mais
    rápida que Registry pub/sub, **mas sem windowing a mailbox explode** (consumidor lento → pico 190.276)
    → **OOM**; com janela fica em 999 (`streaming_bench.exs`); (c) baseline do sistema 657k produce /
    1.2M consume rec/s, memória plana (`throughput_1m.exs`). Alvo NorthGuard-fiel = **push + windowing** +
    **protocolo binário**. Autorizado **remover o modelo de fila legado** (queues/channels) e **substituir**
    o JSON pelo binário. Frentes: **B1** protocolo binário → **B2** streaming push+windowing → **B3** remover
    filas legado. Cada uma fatiada (núcleo testável + fiação).
    - ✅ **B1a — codec binário (`Malachi.Wire`, núcleo puro).** Framing length-prefixed
      (`<<len::32, body>>`), envelope request `<<api_key::16, correlation_id::32, payload>>` / response
      `<<correlation_id::32, error_code::16, payload>>` (o `correlation_id` habilita **pipelining**), e os
      codecs das 4 operações de log (create_topic/produce/fetch/commit). Records **sem offset** no wire (o
      cliente nunca vê offset; o cursor opaco carrega a posição) — encoding próprio, distinto do
      `Record.encode/1` de disco. Cursor/key são byte-strings com flag de presença (`nil` ≠ vazio). Puro;
      a fiação no socket é B1b. Testado: round-trip de frame (+ `:incomplete` em prefixo parcial, dois
      frames num buffer), envelope, wire-record (nil-key vs vazio, bytes não-UTF-8, property de round-trip),
      e as 4 operações.
    - ✅ **B3a — remover o *protocolo* de fila (antes de B1b).** Ordem invertida: mudar o loop de conexão
      para frames binários torna as 14 ações JSON de fila/canal inacessíveis e quebraria seus testes;
      então o legado de *protocolo* de fila sai primeiro, deixando o `tcp_protocol` só com log — aí B1b
      converte log JSON→binário limpo. Removidas as 14 `handle_action` de fila/canal e seus helpers
      (`publish`/`enqueue`/`build_queue_options`/backpressure/…) do `tcp_protocol` (1130→~200 linhas, só
      create_topic/produce/fetch/commit + auth/permissão compartilhados); removido o modo `subscribed`/
      `receive_active_loop` do `tcp_acceptor` (era push de fila — B2 reintroduz um loop de *streaming de
      log* próprio). Os **módulos** `Queue`/`Channel`/etc. ficam (B3b os deleta + ajusta metrics/dashboard).
      Testes: `tcp_queue_management` (100% fila) deletado; os 7 de protocolo/segurança via socket
      (`tcp_protocol`, `channel_integration`, `comprehensive_security`, `protocol_fuzzing`, `rate_limiting`,
      `validation`, `penetration`) **pulados** (`@moduletag :skip`) para reescrever contra o binário em B1b
      (evita migrá-los para um log-JSON que some) — a infra (`Auth`/`RateLimiter`/`Validator`) segue coberta
      por seus unit tests, e o **log e2e** (`log_protocol_test`) continua verde. Suíte: 1087 testes, 0
      falhas, 94 skipped; credo + dialyzer limpos.
    - ✅ **B1b-i — fiação binária (e2e).** O `tcp_acceptor` lê frames `Wire` length-prefixed (o listen
      socket mudou de `packet: :line` para `packet: 0` — raw; o `Wire` faz o framing) num buffer que
      tolera frames divididos em vários `recv` (`decode_frame` → `:incomplete`); o **auth** também virou
      binário (novo `api_key` 0, req username/password → resp token). O `tcp_protocol.process_frame/4`
      decodifica o request **na borda com `try`** (frame malformado → um frame de erro, sem crashar),
      roteia por `api_key` e responde binário via `Wire.encode_ok`/`encode_error` (error_code 0 = ok, 1 =
      erro com a reason como string). `LogApi.produce_records/3` (novo) aceita `%Record{}` direto do wire,
      pulando o passo map→record do `produce/3` JSON; o `fetch_req` ganhou `group` (cursor tem
      precedência). O `TCPHelper` ganhou os helpers binários (`request`/`recv_frame`/`authenticate_wire`)
      e o `log_protocol_test` foi **reescrito** para o binário (o caso de base64 saiu — o valor é bytes
      nativos; o de cursor malformado virou bytes inválidos). Testado: log e2e binário (create/produce/
      fetch/grupos/commit/binário/long-poll/permissões) + `Wire` (auth/ok/error round-trip). Suíte: 1055
      testes, 0 falhas, 94 skipped; credo + dialyzer limpos.
    - ⏳ **B1b-ii (próximo)** — reescrever os 6 testes de protocolo/segurança pulados no B3a
      (`tcp_protocol`, `comprehensive_security`, `protocol_fuzzing`, `rate_limiting`, `validation`,
      `penetration`) contra o `Malachi.Wire`, restaurando a cobertura de segurança sobre o binário;
      deletar `channel_integration` (canal não existe mais) e remover os helpers JSON do `TCPHelper`.
  - 🚧 **Deploy multi-nó/replicado (incremental: D1 → D2 → D3).** As peças de HA já existiam e eram
    testadas isoladamente (SWIM membership, replicação por quórum cross-node, `ra`, self-healing,
    failover); esta fase **liga-as na aplicação**. Descoberta de nós **estática via config** (o SWIM
    detecta falhas em runtime; `libcluster` fica para depois).
    - ✅ **D1 — Control plane HA (metadata via `ra`).** `application.ex` sobe o `Malachi.LogBroker` com
      `metadata_cluster`/`metadata_nodes` quando `:log_cluster` está configurado (env
      `MALACHIMQ_LOG_CLUSTER`/`MALACHIMQ_LOG_NODES`/`MALACHIMQ_RA_DATA_DIR`), iniciando `ra`; **ausente
      = single-node in-memory** (default preservado). A decisão config→opções é uma função pura
      testável (`Application.metadata_cluster_opts/2`). O mecanismo (metadata sobrevive à perda do
      líder) já é provado por `metadata_ha_test`/`broker_server_ra_test` — não duplicado. Também
      **isolado o `log_data_dir` por execução de teste** (`config/test.exs`): o dir fixo persistia entre
      runs e, com metadata in-memory reiniciando, um topic reusado colidia com um segment em disco
      (`Log.ensure_active :already_exists`) → flakiness e2e; agora cada run usa um dir próprio, limpo no
      `after_suite`.
    - ✅ **D2 — Data plane replicado.** Em modo cluster, `application.ex` sobe um `ReplicationServer`
      **nomeado** (`Malachi.LogReplication`) por nó e liga o `LogBroker` a `brokers: [{Malachi.LogReplication,
      n} | n ← log_nodes]` + `replication_factor` (env `MALACHIMQ_LOG_REPLICATION_FACTOR`, default 3,
      clampado ao nº de nós pelo broker). O `Placement` (HRW) escolhe o `replica_set` entre esses brokers;
      o primário replica cross-node via `{name, node}` e commita por **quórum**. Broker set **estático**
      (todos os nós da config); um follower caído é tolerado pelo quórum (o `live_brokers` ao vivo é D3).
      Fiação testável por funções puras (`Application.broker_refs/1`, `data_plane_opts/2`); o mecanismo de
      quórum/tolerância já é coberto por `replication_server_test`, e a integração (BrokerServer + 3 brokers
      + rf=3 + ra → produce/consume por quórum ponta a ponta) por `broker_server_ra_test`.
    - 🚧 **D3 — Membership + healing/failover ao vivo.**
      - ✅ **D3a — `MembershipServer` cross-node.** O SWIM identificava cada membro pelo `self_ref`, que
        era o `:name` de registro — os testes eram todos in-process (átomos únicos). Cross-node isso
        colidia: o mesmo átomo `Malachi.LogMembership` resolve para o servidor **local** em cada nó, então
        o remetente gossipado apontava para o próprio receptor e a view não convergia. Agora o
        `MembershipServer` aceita `:self_ref` (identidade **node-qualified** `{name, node()}`, gossipada)
        distinto do `:name` (registro local), + `start/1` (não-linkado, p/ iniciar em nós remotos). Provado
        por **teste multinode** real (`membership_ha_test`): 3 nós `:peer` convergem via seeds e detectam a
        morte de um nó (SWIM: suspeita → dead → sai do alive set).
      - ✅ **D3b — Fiação na app.** Em modo cluster, `application.ex` sobe (nesta ordem)
        `MembershipServer` → `ReplicationServer` → `LogBroker` → `HealCoordinator`. O `MembershipServer`
        usa `self_ref: {Malachi.LogMembership, node()}` e `peers: membership_seeds(log_nodes)` (os outros
        nós). O `live_brokers` (fun) deriva de `alive_members` → refs `{Malachi.LogReplication, node}`, e é
        passado ao `LogBroker` (que estreita o placement de novos segments ao conjunto vivo; `:brokers`
        estático é o placement inicial) **e** ao `HealCoordinator`. O `HealCoordinator` (`metadata_source:
        BrokerServer.metadata`, `apply_command: BrokerServer.apply_heal`, `replication_factor`) fecha o loop
        *broker morre → membership marca gone → segments re-replicados + primário promovido*. Fiação pura
        testável (`membership_seeds/1`, `live_replication_refs/1`); o loop de healing/failover já é coberto
        por `heal_coordinator_test`/`self_healing_test`/`failover_test`, e o membership cross-node por D3a.
  - ✅ **Deploy multi-nó/replicado completo** (D1 control plane HA + D2 data plane replicado + D3
    membership/healing/failover ao vivo). Ligado por config estática; `libcluster` (descoberta dinâmica)
    fica como conveniência futura.
- 🚧 **C — Features NorthGuard restantes.** Decisão: começar por **C1 — retenção (tempo+tamanho)**;
  attributes (C2) e policies (C3) depois. Design aprovado: `sealed_at` explícito no segment (idade),
  retenção por tamanho **por range**, e consumidor num dado expirado **avança para o início disponível**
  (mantém o cursor opaco). Sub-fatias: C1a (primitiva de delete) → C1b (coordenador + política + fiação).
  - ✅ **C1a — control plane.** `segment_meta` ganha `sealed_at` (epoch ms, `nil` enquanto ativo); o
    comando `seal_segment` carrega o `sealed_at` (gerado no `Broker` como os timestamps de `Record`, então
    determinístico entre réplicas). Novo comando `{:delete_segment, segment_id}`: remove um segment
    **selado** do control plane (`:segment_active` se ainda ativo — nunca dropa o ativo; `:no_such_segment`
    se ausente). Testado: unit do `delete_segment` (selado/ativo/inexistente), `sealed_at` no seal,
    determinismo preservado (property tests).
  - ✅ **C1a — storage delete.** Cada segment do broker é um `Log` num subdiretório próprio, e o
    `ReplicationServer` guarda `logs: %{segment_id => Log}`; então deletar é **fechar o `Log` + apagar o
    diretório** — sem precisar de delete granular no `SegmentStore`. `Log.delete/1` (fecha + `rm_rf` do
    diretório, best-effort). `ReplicationServer.delete/2` (client + handle_call): fecha/apaga se o log
    está aberto, senão limpa arquivos órfãos em disco (pós-restart); **idempotente** (deletar um segment
    desconhecido é `:ok`). Testado: `Log.delete` (diretório some), `ReplicationServer.delete` (dados
    somem → read vira `:eof`; idempotência).
  - 🚧 **C1b — coordenador + read path + fiação** (incremental).
    - ✅ **C1b-1 — política + `RetentionCoordinator`.** `segment_meta` ganha `byte_size` (via `seal_segment`,
      do `active.bytes` do Broker — determinístico, como `sealed_at`; retenção por tamanho precisa de bytes).
      Módulo **puro** `Malachi.Cluster.Retention`: `expired(metadata, now_ms, policy)` → ids de segments
      **selados** a expirar por **idade** (`sealed_at` > `max_age_ms`) e por **tamanho** (soma `byte_size`
      por **range** > `max_bytes` → mais antigos primeiro), unidos; nunca o ativo; bound `nil` desliga a
      regra. `RetentionCoordinator` (GenServer periódico, modelo `HealCoordinator`) com seams
      (`metadata_source`, `expire_segment`, `policy`, `clock`, `interval`): cada sweep resolve os ids para
      seus metas e chama `expire_segment`. Testado: `Retention` (idade, tamanho por-range, união, nunca o
      ativo, `nil` desliga) e o coordenador (sweep via `run_now` e via tick, meta completa ao seam).
    - ✅ **C1b-2 — read path (avança em dado expirado).** Retenção deleta sempre um **prefixo contíguo**
      (os segments mais antigos), então o read path só precisa saber o menor `start_offset` ainda armazenado
      (`earliest_offset/2`) e **clampar o offset a ele** antes de ler. `consume_page` (consumo ao vivo) e
      `read_history_page` (admin) clampam: um consumidor cujo cursor caiu num buraco expirado avança
      transparentemente para o dado vivo (at-least-once, cursor opaco intacto) em vez de ver `:eof` enganoso;
      como não há buracos internos, `offset + length` continua correto (sem mudar `read`/`locate_segment`).
      Testado: consumidor abaixo do earliest pula os segments expirados e lê o que resta.
    - ✅ **C1b-3 — config + fiação (retenção C1 completa).** `Broker.delete_segment/2` +
      `BrokerServer.delete_segment/2` (aplicam `:delete_segment` pelo control plane, Raft-backed quando
      configurado). A `expire_segment` real (na `application.ex`): remove do control plane e então deleta
      o storage em cada réplica (`ReplicationServer.delete`), **best-effort** (control plane idempotente,
      storage tolera segment ausente). Config por env: `MALACHIMQ_RETENTION_MAX_AGE_MS` /
      `MALACHIMQ_RETENTION_MAX_BYTES` (ambos ausentes = **guarda para sempre**, coordenador não sobe) /
      `MALACHIMQ_RETENTION_INTERVAL_MS`. O `RetentionCoordinator` sobe na árvore **sempre que há política**
      (importa single-node também), após o `LogBroker`. Testado: `BrokerServer.delete_segment` (drop do
      selado), e **e2e** (produce → sela → sweep do coordenador → segment some do control plane **e** do
      storage). **C1 (retenção tempo+tamanho) completa.**
- 🚧 **C2 — Attributes** (k/v opacos que o admin liga a brokers; base de rack/DC-awareness).
  **Decisão:** disseminar via **Membership/SWIM** (fiel ao NorthGuard: "membership piggyback host/port/
  attributes"), não no Metadata — o usuário priorizou fidelidade. Incremental: C2a (Membership puro) →
  C2b (server + API + gossip) → C2c (fiação + config).
  - ✅ **C2a — `Membership` puro com attributes.** Os attrs de um membro **viajam com o update**,
    governados pela mesma **incarnation**: o `update` vira 4-tupla `{member, status, incarnation,
    attributes}` e o `member_state` ganha `attributes`. Só o dono muda seus attrs — via `set_attributes/2`,
    que **sobe a própria incarnation** para a mudança vencer o merge em todo lugar; uma suspeita/confirmação
    de outro nó carrega os attrs **já conhecidos** (preserva-os). O `overrides?` (precedência `{incarnation,
    rank}`) não muda. `new/2` aceita `:attributes` do self; query `attributes/2`. Testado: propagação/troca
    por incarnation, `set_attributes`, preservação em suspect, gossip via `updates`; convergência
    order-independent preservada (property; attrs são consistentes por-incarnation, então os generators
    usam `%{}`). Multinode SWIM (D3a) segue convergindo com a 4-tupla.
  - ✅ **C2b — `MembershipServer` com attributes.** Opção `:attributes` (attrs iniciais do self,
    passados ao `Membership.new`); API `set_attributes/2` (muda os próprios attrs em runtime — sobe a
    incarnation) e `attributes/2` (lê os de um membro). Disseminação é **passiva** (anti-entropy): o
    server ignora os effects e o gossip periódico (ping/ack piggyback `updates`) propaga — nenhum push
    proativo, consistente com o resto do server. Testado: attrs iniciais legíveis, e `set_attributes` num
    nó propaga a um peer via gossip.
  - ✅ **C2c — fiação na app (C2 completa).** `application.ex` liga os attributes do self no
    `MembershipServer` do cluster: `MALACHIMQ_LOG_ATTRIBUTES` (formato `"rack=a,dc=east"`) é parseado por
    `Application.parse_attributes/1` (função pura testável: ignora entradas sem `=`, trima, preserva `=` no
    valor) e passado como `:attributes`. Ausente → `%{}`. Testado: parse (vazio, pares, trim, entradas
    inválidas, valor com `=`). **C2 (attributes via SWIM) completa** — os brokers disseminam seus attrs por
    gossip, prontos para o placement rack-aware de C3.
- 🚧 **C3 — Policies** (nome + retenção + constraints sobre attributes → replica sets; fiel ao NorthGuard,
  que unifica tudo em *policies*). Incremental: C3a (Placement puro com spread) → C3b (integração: attrs do
  membership → placement) → C3c (policies por-topic: definição + associação + retenção por-topic).
  - ✅ **C3a — `Placement` puro com spread (rack-aware).** `place/4` ganha a opção `:spread =
    {attribute_key, attributes}`: sobre o ranking HRW (determinístico), faz **round-robin pelos valores
    distintos** do attribute — o melhor-rankeado de cada valor primeiro, depois o próximo de cada, até `rf`.
    Com `rf ≤ nº de valores`, cada réplica num rack/DC distinto; senão best-effort round-robin. Determinístico
    (ranking HRW + agrupamento estável); sem `:spread` → top-`rf` de antes (todos os callers de `place/3`
    intactos). Testado: valores distintos, prioriza diversidade sobre rank puro (rf=2 em a,a,b → a,b),
    best-effort com rf > valores, brokers sem o attribute agrupam à parte, determinismo.
  - ✅ **C3b — integração (attributes → placement).** O `Broker` ganha `spread_by` (a chave de attribute)
    e `broker_attributes` (map broker→attrs); `open` os aceita, `set_broker_attributes/2` os atualiza (como
    `set_brokers`), e `open_segment` passa `:spread` ao `Placement.place` quando `spread_by` está setado
    (senão placement normal — todos os callers intactos). O `BrokerServer` aceita `:spread_by` + uma fun
    `:broker_attributes` e a **refresca periodicamente** (mesmo timer de `:live_brokers`) para o broker —
    então os attrs disseminados pelo membership (C2) fluem ao placement. Testado: produce espalha réplicas
    por rack, `set_broker_attributes` afeta o placement seguinte, refresh do `BrokerServer` puxa os attrs.
  - ✅ **C3c-1 — fiação do rack-aware na app.** `data_plane_opts` liga `spread_by` (env
    `MALACHIMQ_LOG_SPREAD_BY`, ex: `"rack"`) e uma fun `broker_attributes` derivada do `MembershipServer`:
    `broker_attributes_for/2` (pura, testável) mapeia cada membro vivo `{LogMembership, node}` →
    `{LogReplication, node}` com os attrs gossipados (C2). Com isso o **placement rack-aware funciona ponta
    a ponta na aplicação** (attrs do membership → spread do placement). Ausente `spread_by` → HRW puro.
  - ✅ **C3c-2 — policies por-topic** (o guarda-chuva NorthGuard). Decisão: policies **dinâmicas no
    Metadata (`ra`)** + escopo **ambos** (retenção + placement por-topic). Incremental: 2a (Metadata:
    policies + associação) → 2b (retenção por-topic) → 2c (placement por-topic). **Fecha C3.**
    - ✅ **C3c-2a — Metadata: policies + associação.** `Metadata` ganha `policies: %{name => policy}`
      (`policy = %{optional(:retention) => %{max_age_ms, max_bytes}, optional(:spread_by) => term}`), o
      `topic_meta` ganha `policy: name | nil`, e dois comandos: `{:define_policy, name, policy}` (valida
      name binário não-vazio + policy map; `:invalid_policy` senão) e `{:set_topic_policy, topic,
      name | nil}` (`:no_such_topic`/`:no_such_policy`; `nil` desassocia → volta aos globais). Queries
      `get_policy/2` e `topic_policy/2` (resolve a policy do topic). Determinismo preservado (property).
      **Sem uso ainda** — 2b/2c ligam retenção/placement à policy do topic.
    - ✅ **C3c-2b — retenção por-topic.** `Retention.expired/3` passa a resolver, **por range**, a retenção
      efetiva = a `:retention` da policy do topic (`Metadata.topic_policy/2`) **mesclada sobre** a policy
      global (a policy sobrepõe só as chaves que define; `Map.merge`), ou a global quando o topic não tem
      policy. O `RetentionCoordinator` não muda (já passa o metadata + a global). Testado: policy do topic
      sobrepõe a global, merge (chave não-definida cai na global), topic sem policy usa a global.
    - ✅ **C3c-2c — placement por-topic.** `Broker.open_segment` resolve, por range, o `spread_by`
      efetivo = o `:spread_by` da policy do topic (`Metadata.topic_policy/2`) quando a policy **define**
      essa chave (`nil` explícito opta o topic **fora** do spread — rendezvous puro), sobrepondo o global;
      senão o `spread_by` global do broker. Simétrico ao 2b (chave definida vence, `nil` incluso). Só
      `place_opts/effective_spread_by` mudam. Testado: policy liga o spread sobre um global-off; `nil`
      explícito desliga sobre um global-on (== rendezvous puro).
- 🚧 **D — Sharding via `ReplicatedDSRSM`** (agora **no alvo**): metadata sharded (um cluster `ra`
  por vnode) para **escalar o control plane** além de um cluster Raft único. Decisão: **1A** — o cache
  do `Broker` vira um `DSRSM` (espelha o par `Metadata`/`ReplicatedMetadata`), roteando leituras/escritas
  por topic; e **2A** — incremental, núcleo puro primeiro. Infra já pronta: `HashRing`, `DSRSM` (puro),
  `ReplicatedDSRSM` (ra), `MetadataMachine`/`MetadataServer`.
  - ✅ **D-a — `Broker` sobre `DSRSM` (in-memory, 1 vnode).** O cache do `Broker` deixa de ser um
    `Metadata` e passa a ser um `DSRSM` (`broker.dsrsm`), roteado por topic (derivável do `range_id`
    `{topic, seq}`/`segment_id`). Novo combinador puro `DSRSM.update_vnode/3` (roteia + aplica uma
    função ao `Metadata` do vnode); `DSRSM.command/3` delega a ele com `&Metadata.apply/2` (property
    tests intactos). `command_fun` do `Broker` vira `(DSRSM, topic, command) -> {DSRSM, reply}` (default
    `&DSRSM.command/3`); `apply_metadata` deriva o topic via `command_topic/1`. Acessores novos no
    `DSRSM`: `single/1` (forma trivial 1-vnode p/ seed), `committed_offsets/3`, `topic_policy/2`,
    `merged_metadata/1` (união dos shards → `Broker.metadata/1` p/ retention/healing). No `BrokerServer`,
    o caminho Raft embrulha o cluster único como `DSRSM.single(seed)` + um `command_fun/3` que injeta
    `ReplicatedMetadata.apply_command` no `update_vnode` (D-b troca pelo `ReplicatedDSRSM` real). Com 1
    vnode, comportamento idêntico: suite completa verde (981) como rede de segurança.
  - ✅ **D-b** (runtime/`BrokerServer` sobre `ReplicatedDSRSM`, N vnodes). Decisão: **1A** — D-b-1
    (N vnodes **single-node**) primeiro; HA-por-vnode (D-b-2) depois.
    - ✅ **D-b-1 — control plane sharded single-node.** `BrokerServer` ganha o caminho `:metadata_vnodes`
      (`[{cluster_name, token}]`): inicia N clusters `ra` via `ReplicatedDSRSM` (um por vnode), materializa
      o cache local com `ReplicatedDSRSM.snapshot/1` (novo — lê o `Metadata` de cada vnode → `DSRSM.seed/2`,
      novo, compartilhando o ring), e um `command_fun/3` sharded que roteia por topic ao cluster `ra` do
      vnode (`ReplicatedDSRSM.server_for/2`, novo) aplicando via `ReplicatedMetadata.apply_command` no
      `update_vnode`. O caminho `:metadata_cluster` (1 vnode, D-a/D1) segue intacto. Config: `log_vnodes`
      (inteiro N; `Application.sharded_vnodes/2` gera N vnodes com tokens uniformes no ring de 32 bits).
      Testado: 2 vnodes, cada topic vive em exatamente o cluster `ra` que seu nome roteia (nunca no outro),
      e os topics se distribuem entre os vnodes; `sharded_vnodes/2` (tokens distintos, em range).
    - ✅ **D-b-2 — HA por vnode.** `ReplicatedDSRSM.add_vnode/4` passa a receber os `nodes`, iniciando o
      cluster `ra` de cada vnode sobre eles (`MetadataServer.start/2`), então cada vnode sobrevive à perda
      de um membro. Modelo: **todos os vnodes sobre o mesmo conjunto de M nós** (espelha o D1; placement de
      vnodes por subconjuntos de nós fica para depois). `BrokerServer` passa os `metadata_nodes` ao caminho
      sharded (`start_vnodes/2`); `Application.metadata_opts` inclui `metadata_nodes` no caminho sharded.
      `snapshot/1` usa `&Function.identity/1` (query linearizável roda no líder, possivelmente remoto).
      Testado (`:multinode`): 2 vnodes sobre 3 nós, mata um membro do vnode dono (o líder se for peer →
      failover; senão um follower), o vnode ainda commita e os metadados (dele e do outro vnode) intactos.
  - 🚧 **D-c — gestão do control plane por vnode** (retention/healing/failover). **Estado atual:** as
    *escritas* de metadata já são sharded (D-b), mas a *gestão* segue **centralizada** — um
    `RetentionCoordinator` e um `HealCoordinator` no nó do `BrokerServer` leem `merged_metadata` (a
    **união** de todos os shards) e emitem comandos (`delete_segment`/`set_segment_replicas`) que
    **roteiam de volta** por topic ao vnode dono (via `command_topic/1`). Isso é **correto** sob
    sharding (a união é exata; os comandos roteiam), mas reintroduz conceitualmente o ponto único que
    o sharding elimina — um **débito de fidelidade de sequenciamento**, não de correção.

    O alvo fiel ao NorthGuard é **1C: um coordinator vivendo na liderança do grupo Raft de cada vnode**
    (cada nó gerencia retention/healing dos vnodes que lidera). Isso **pertence à Fase 1** (distribuição),
    **não** à Fase 2 (eficiência nativa/profiling). O motivo de 1C não vir já não é ser "otimização",
    e sim ter **pré-requisitos**:
      1. **Placement de vnodes por subconjuntos de nós** (hoje todos os vnodes vivem nos mesmos M nós —
         adiado em D-b-2). Sem espalhar os vnodes, "o líder do vnode" é qualquer um dos M nós e há pouca
         distribuição real a fazer. **Fatia D-c-1** (decisão: **1A** HRW reusando `Placement`; **2A**
         núcleo puro primeiro):
           - ✅ **D-c-1a — núcleo puro.** `Application.place_vnodes/3` atribui a cada vnode
             (`{vnode_id, token}`) os `R` nós do seu cluster `ra`, escolhidos de `nodes` por rendezvous
             (o mesmo HRW `Placement.place/4` dos segments) → `{vnode_id, token, nodes}`; determinístico,
             mínimo movimento, `R` efetivo = `min(R, M)`. Testado isolado (HRW espalha, determinismo,
             clamp). **Sem uso ainda** — D-c-1b liga ao `ReplicatedDSRSM`/`BrokerServer`.
           - ✅ **D-c-1b — roteamento cross-node.** `MetadataServer.start/2` passa a devolver o server de um
             **membro real** (o nó local quando é membro; senão o primeiro do placement) em vez do local
             sempre — então um vnode colocado num subconjunto de nós é alcançável de um nó que **não** hospeda
             réplica dele (o `ra` roteia `command`/`query` desse membro ao líder; o chamador não precisa ser
             membro). `ReplicatedDSRSM` armazena esse server; `command`/`query`/`snapshot`/`server_for`
             passam a funcionar cross-node. Testado (`:multinode`): 2 vnodes em subconjuntos **disjuntos** de
             3 nós, orquestrados de um nó que **não** hospeda nenhum — commits/queries roteiam ao membro certo
             e o `snapshot` materializa tudo. (Decisão **1A**: mecanismo isolado do bootstrap distribuído.)
           - ✅ **D-c-1c — bootstrap distribuído (seed estático).** `Application.metadata_opts` liga o
             `place_vnodes` (`metadata_vnodes` vira `[{vnode_id, token, nodes}]`, R = `log_vnode_replication_factor`)
             e injeta a política `bootstrap_orchestrator?` = `Application.static_seed/1` (verdade só no menor nó).
             No `BrokerServer`, o **orquestrador** faz `add_vnode` (start_cluster) de cada vnode; os **não-orquestradores**
             fazem `ReplicatedDSRSM.route_vnode/4` (novo — registra no ring + server de um membro, **sem** iniciar), de
             modo que exatamente um nó bootstrapa cada vnode (padrão RabbitMQ/`ra`). O `snapshot/1` ficou **tolerante**
             (vnode não-pronto → `Metadata` vazio, sem crash) e o `BrokerServer` **re-seeda o cache** dos clusters `ra`
             logo após o boot (janela de eleição) e periodicamente (`Broker.put_cache/2`), o que também cobre o
             multi-writer. Escolha **1B/seed estático** (vs 1A concorrente, arriscado no `ra`; vs orquestração-pelo-líder,
             que é a D-c-1d com fencing). Testado: `static_seed` (só o menor nó), `route_vnode` + `snapshot` tolerante
             (single-node), e `:multinode` — orquestrador inicia sobre 2 nós, não-orquestrador só roteia e lê/escreve
             cross-node. **Config:** `MALACHIMQ_LOG_VNODE_REPLICATION_FACTOR`.
           - ✅ **D-c-1d — `membership_leader` + reconcile loop.** A política de orquestração passa do seed
             estático para `Application.membership_leader/1` — verdade só no menor nó **vivo** (`MembershipServer.
             alive_members`, SWIM), então o papel **faz failover** quando o líder cai (tolerante: se a membership
             não responde → não-líder, nunca dois). O bootstrap vira **reconcile** (controller-style, k8s): no
             boot **todo** nó só faz `route_vnode` (`build_replicated` sem `start`); o `BrokerServer` reconcilia
             (level-triggered, idempotente) logo após o boot e periodicamente — e **só no líder** — chamando
             `MetadataServer.start/2` nos vnodes cujo cluster ainda não está pronto (`MetadataServer.ready?/1`,
             novo). O **fencing** é o nome do cluster `ra` (um segundo `start` do mesmo vnode falha sem
             duplicar — validado empiricamente); o *lease* (jeito k8s literal) fica para o **1C**, onde o líder
             passa a fazer trabalho **contínuo** (retention/healing/rebalancing). `static_seed/1` permanece como
             alternativa (testada). Testado: `membership_leader` (menor-vivo, tolerância); integração — o líder
             bootstrapa os vnodes via reconcile e um `create_topic` commita. Ver seção 8 (referências k8s/riak_core).
      2. **Detecção/reação a liderança Raft por vnode** — um supervisor que sobe/derruba coordinators
         conforme a liderança muda (via eventos do `ra`), tolerando oscilação e split-brain momentâneo.
    Sequência: D-b ✅ → **D-c-1 placement de vnodes** ✅ → **1C-a coordinators só-no-líder** ✅ →
    **1C-b-i detecção de liderança Raft por vnode** ✅ → **1C-b-ii-α coordinator apontado a um vnode** ✅
      → **1C-b-ii-β supervisor/manager per-vnode-leader** ✅. **1C-b completo.**

    - ✅ **1C-a — coordinators só-no-líder (sem lease).** `RetentionCoordinator` e `HealCoordinator`
      ganham o seam `:leader?` (`(-> boolean())`, default sempre); a cada tick, só varrem/curam se
      `leader?()` — senão o tick corre mas pula. O `Application` injeta `membership_leader(Malachi.
      LogMembership)` (reusa D-c-1d) nos dois quando clustered (`coordinator_leader?/1`; single-node =
      sempre age). Elimina a **redundância** (N nós faziam o mesmo trabalho) mantendo o modelo
      level-triggered. **Sem lease:** o trabalho é idempotente + roteado ao `ra` (serial), então dois
      coordinators transitórios (convergência SWIM) só refazem trabalho, não corrompem — o mesmo
      raciocínio do bootstrap. `run_now/1`/`heal_now/1` ignoram o gate (triggers manuais). Testado:
      não-líder pula o tick; o trigger manual age mesmo assim.
    - ✅ **1C-b-i — detecção de liderança Raft por vnode (núcleo puro).** `MetadataServer.leader?/1`
      espelha `ready?/1`: lê o líder que `:ra.members` reporta (qualquer membro alcançável responde) e
      é verdade só se ele for o **próprio** `server_id` — passe o server **local** (`{vnode_id, node()}`)
      para perguntar "este nó lidera este vnode?". Cluster não-formado/inalcançável → false (nunca
      assume liderança). `Application.leading_vnodes/3` é o seletor puro: dado o placement
      (`[{vnode_id, token, nodes}]` do bootstrap), o nó local e o predicado `leader?` (default
      `MetadataServer.leader?/1`), retorna os vnodes que o nó **hospeda** (placement o inclui) **e**
      **lidera** — curto-circuitando `leader?` para vnodes não-hospedados. É onde 1C-b-ii vai rodar os
      coordinators, um por vnode, no líder Raft dele (o NorthGuard literal, distribuindo a carga vs o
      líder único de membership do 1C-a). Testado: `leader?` no líder single-node (ra real) + não-formado
      → false; `leading_vnodes` filtra host×lidera, preserva ordem, não consulta liderança de não-hospedado.
    - ✅ **1C-b-ii-α — coordinator apontado a um vnode.** `Application.vnode_metadata_source/1` é um
      `metadata_source` ligado a **um** vnode: lê a visão local do `Metadata` daquele vnode via
      `MetadataServer.query({vnode_id, node()}, & &1)` (consistent query ao ra do vnode), **tolerante**
      (vnode não-formado/inalcançável → `Metadata.new()`, sem crashar o coordinator). Como o tipo é o
      mesmo (`Metadata.t()`) e `expire_segment`/`apply_heal` já roteiam ao vnode dono por topic, o
      `RetentionCoordinator`/`HealCoordinator` **não mudam** — basta trocar o source (por-vnode em vez do
      merge global) e o gate (`MetadataServer.leader?({vnode_id, node()})`). Testado (ra real): source
      tolerante em vnode não-formado; lê só o shard do vnode; um `RetentionCoordinator` ligado a um vnode
      expira só os segments **daquele** vnode.
    - ✅ **1C-b-ii-β — supervisor/manager per-vnode-leader.** `Malachi.Cluster.VnodeCoordinatorManager`
      (GenServer genérico, testável por seams `leading`/`spawn`/`stop`) reconcilia por **polling
      level-triggered** (logo após o boot via `handle_continue`, depois a cada `:vnode_reconcile_interval_ms`,
      default 5s): compara os vnodes que este nó lidera agora (`leading_vnodes/3` sobre `MetadataServer.
      leader?`) com os que já roda, **sobe** um par retention+heal para os recém-liderados e **derruba** os
      que deixou de liderar. Cada par vive sob um **supervisor por-vnode** (`:one_for_one`) num
      `DynamicSupervisor` (`Malachi.LogVnodeCoordinatorSupervisor`), para que um coordinator que crashe
      reinicie sem o manager perder o handle. No `Application`, `coordinator_children/2` **substitui** os
      coordinators únicos do 1C-a pelo par supervisor+manager **quando sharded** (`vnode_placement/2` ≠
      nil, extraído e reusado por `metadata_opts`); single-node / cluster de 1 vnode seguem no 1C-a.
      Cada coordinator mantém o gate `MetadataServer.leader?({vnode_id, node()})` como **defesa em
      profundidade** (se o manager atrasar numa oscilação, o coordinator não age após perder a liderança).
      **Idempotente + sem lease** (mesmo raciocínio do 1C-a): um flap transitório só refaz trabalho
      roteado ao `ra`, não corrompe. Testado: reconcile por seams (sobe/derruba/idempotente/esvazia) +
      integração (ra real, single-node) — o manager sobe um `RetentionCoordinator` real por vnode liderado
      e cada um expira só os segments do **seu** vnode. O **lease sobre `ra`** fica para quando o
      coordinator ganhar trabalho **não-idempotente** (rebalancing com movimento de dados).

    (A alternativa **1B** — coordinators iterando por-vnode mas ainda centralizados — evita materializar
    o merge, mas é um meio-termo sem gargalo medido; preterida em favor de ir direto ao placement.)

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

---

## 8. Referências de design externas: riak_core e Kubernetes

Analisamos três repositórios **riak_core** (a biblioteca de consistent hashing / vnodes / membership /
handoff do Riak) e o **Kubernetes**, para aprender como sistemas maduros resolvem os problemas que a
Fase 1 (distribuição) enfrenta: bootstrap distribuído, leader election com fencing, placement com
tolerância a falha, e reconcile loops.

**Enquadramento comum.** Os três convergem no mesmo padrão para coordenar shards: um **coordenador único
eleito**, com **fencing via consenso**. RabbitMQ (mesma lib `ra` que usamos) fencia pelo **nome do
cluster**; riak_core usa um *claimant* (fencing **fraco**, gossip); k8s usa um *Lease* sobre etcd
(fencing **forte**, CAS linearizável). **Veredito geral: referência de design, não dependência** — o
riak_core é **AP** (gossip + vector clocks; consenso forte só no `riak_ensemble`, externo) e o k8s
centraliza o metadata num **único** cluster **etcd** (Raft, não-sharded). O malachi é **CP e sharded**
(um cluster `ra` por vnode), então nenhum serve como dependência direta, mas os **algoritmos e padrões
são portáveis** — trocando "gossip" por "Raft" e preservando determinismo.

### 8.1 riak_core (AP) — referência: OpenRiak (`~/riak_core`, Apache 2.0, fork ativo)

- **Onde o malachi já está à frente (por ser CP):** metadata em Raft (> gossip); SWIM com suspicion
  (> gossip simples); failover automático; retention; atributos rack/DC. *Não regredir ao portar.*
- **`riak_core_claimant` → D-c-1d + rebalancing:** o modelo **staged → planned → committed** (o *plan*
  computa o ring novo sem mudar estado; o *commit* valida que nada divergiu) + eleição **lexicográfica**
  (menor node = o nosso `static_seed`). O fencing do claimant é **fraco** (gossip + vclock, split-brain
  possível) — o que **valida** a decisão do malachi de fenciar o bootstrap pelo **nome** do cluster `ra`.
- **`riak_core_claim_binring` V4 → upgrade do `place_vnodes`:** garante réplicas em nós **e** *locations*
  (rack/DC) distintos (`target_n_val`), balanceamento uniforme (k ou k+1 por nó) e rebalanceamento com
  **movimento mínimo** (`update()` antes de `solve()`). Doc bem comentada: `~/riak_core/docs/claim-version4.md`.
- **Ring versioning + ciclo de claim → re-clustering dinâmico** (add/remove nós; hoje é `add_vnode` manual).
- **Ignorar:** gossip do ring, vector clocks (o Raft já dá ordem/consenso), `node_watcher` (o SWIM
  resolve), preflist-sobre-vnodes (desvio intencional — o malachi sharda **metadata por topic**, não
  distribui keys de dados sobre o ring).

### 8.2 Kubernetes (CP via etcd único)

- **Leader election + Lease → D-c-1d e, sobretudo, 1C (coordinators no líder):** o "triângulo"
  `LeaseDuration > RenewDeadline > RetryPeriod` + **CAS linearizável** (etcd; o nosso `ra` dá o mesmo) +
  **desistir proativo** ao não conseguir renovar (`OnStoppedLeading` — evita split-brain) + **relógio
  local** (tolera clock skew; premissa: NTP, drift ≤ ~`lease/10`). Traduz para um **lease armazenado num
  cluster `ra`** (comando CAS versionado). **Nuance-chave:** o fencing-por-nome do `ra` **basta para o
  bootstrap** (start único, auto-fencido); o **lease** só é necessário para o **trabalho contínuo** do
  líder — retention/healing/rebalancing (o 1C). Arquivos: `client-go/tools/leaderelection/`
  (`leaderelection.go`, `resourcelock/leaselock.go`), `api/coordination/v1/types.go`.
- **Controller/reconcile → coordinators + reconcile de bootstrap:** **level-triggered** (reconciliar o
  estado completo *desejado × atual* — os coordinators do malachi já fazem isso); **idempotência**;
  **workqueue** com dedupe + rate-limit + retry/backoff; **expectations** (rastrear operações em voo com
  TTL, para não re-agir cedo demais); **só-o-líder-age**; **observabilidade** (healthz de convergência).
  Referência: `pkg/controller/replicaset/replica_set.go`, `client-go/util/workqueue`.
- **Scheduler / PodTopologySpread → `place_vnodes`:** `topologyKey` (= o nosso `spread_by`/atributos),
  **`maxSkew`** (desbalanceamento máximo entre racks/zonas), **`minDomains`** (domínios distintos
  mínimos), **`whenUnsatisfiable`** (hard `DoNotSchedule` vs soft `ScheduleAnyway`), e pipeline
  **Filter → Score**. **Ressalva crítica:** o k8s **randomiza** o tie-break; o `place_vnodes` deve
  permanecer **determinístico** (raft-safe: toda réplica computa o mesmo placement). Portar as *ideias*
  (maxSkew / minDomains / hard-soft) sobre funções puras, sem randomização; e adotar *sticky preference*
  no `heal()` (preferir réplicas sobreviventes → menos churn). Referência:
  `pkg/scheduler/framework/plugins/podtopologyspread/`.
- **Trade-off registrado:** o etcd único é simples, mas é o **gargalo de escala** do k8s (~5000 nodes por
  cluster). O sharding do malachi paga complexidade (bootstrap / leader / fencing) justamente para
  **escalar além de um quórum** — a motivação da fatia D.

### 8.3 Síntese — como isso informa as próximas fatias

- **D-c-1d (`membership_leader`):** eleição pelo menor nó **vivo** (SWIM) + fencing por nome do `ra` no
  bootstrap; reconcile loop **level-triggered / idempotente** (padrão *controller* do k8s).
- **1C (coordinators no líder):** aqui entra o **Lease sobre `ra`** (k8s) para fenciar o trabalho
  **contínuo**, e o modelo **staged / planned / committed** (riak_core claimant) para mudanças de ring.
- **Upgrade do `place_vnodes`:** `target_n_val` / location-awareness (riak_core binring) + `maxSkew` /
  `minDomains` / hard-soft (k8s topology spread), **mantendo o determinismo**.
  - ✅ **A1 — rack-spread (feito).** `place_vnodes/4` ganha `place_opts`, repassado a `Placement.place/4`;
    com `[spread: {attribute_key, node_attributes}]` as R réplicas de cada vnode caem em **racks/zonas
    distintos** (`target_n_val`/`minDomains`), então perder um rack inteiro não leva a maioria das réplicas
    de um vnode. Reusa o `spread` já existente (round-robin por valor de atributo, do C3a). A topologia é
    **config estática** (`log_topology`, `"node=rack,..."`, `parse_topology/1`), idêntica em todo nó →
    placement **determinístico**. `Application.metadata_opts` liga o spread via `vnode_place_opts/0`
    (`:log_spread_by` + `:log_topology`). Testado: cada vnode com R=2 abrange os 2 racks; determinismo.
  - ✅ **A2 — balanceamento global de carga (`maxSkew`).** `Placement.place_balanced/4` coloca o
    **conjunto inteiro** de vnodes com **carga limitada** (visão global, não por-vnode): cada vnode ainda
    rankeia por HRW, mas um nó no teto (`ceil(total/nós) + max_skew - 1`) é **pulado** para o próximo, de
    modo que nenhum nó fica sobrecarregado. Determinístico (ranking + ordem + contadores iguais em todo
    nó). O teto é **best-effort** — o RF **nunca** é sacrificado por balanceamento, então um vnode que não
    alcançaria `min(rf, nós)` réplicas distintas pega o nó menos-carregado mesmo acima do teto (possível
    com `rf > 1`, onde o greedy por-vnode não empacota perfeito); com `rf = 1` o teto é **rígido**. Com
    `max_skew` grande degrada a HRW puro (movimento mínimo). É **standalone** (não combina com o
    `:spread` do A1 — mutuamente exclusivos; A2 tem precedência). `place_vnodes/4` usa `place_balanced`
    quando `[max_skew: n]`; `vnode_place_opts` liga via `:log_max_skew` (`MALACHIMQ_LOG_MAX_SKEW`).
    Testado: HRW puro empilha 6 de 9 vnodes num nó, balanceado espalha 3/3/3; property do teto rígido
    (rf=1) e de que todo vnode recebe `min(rf,nós)` distintas; determinismo; degradação a HRW com folga.
    Resolve **carga**, não perda de dados (a segurança de rack é o A1).
- **Rebalancing dinâmico** — quando a membership muda (nó entra/sai), redistribuir os vnodes ao vivo.
  Escopo: **control plane** (os membros dos clusters `ra` de cada vnode); adicionar/remover membro
  (`:ra.add_member`/`:ra.remove_member`) faz o **próprio `ra` transferir o estado** (Raft log/snapshot),
  então não movemos dados de metadata à mão; o **data plane** (segments) já é coberto pelo *healing*
  (1C-b). Modelo **manual** *staged → planned → committed* (riak_core; gatilho automático fica para
  depois, por cima do mesmo motor). Decomposto em:
  - ✅ **R1 — `desired_placement` (núcleo puro).** `Application.desired_placement/5` recomputa o placement
    desejado sobre um conjunto de nós arbitrário (a membership **viva**, vs a config estática `:log_nodes`):
    compõe `sharded_vnodes/2` (vnodes lógicos fixos) + `place_vnodes/4` (HRW). Determinístico e
    **movimento mínimo** — um vnode só muda se **adotar** um nó que entrou ou **detinha** um que saiu; o
    resto fica posto. Testado: determinismo; ao **adicionar** um nó, um vnode só muda se adota o novo nó
    (e algum adota); ao **remover**, só muda quem o detinha (e ele some do placement); clamp a `min(rf, |nós|)`.
  - ✅ **R2 — plano de rebalanceamento (núcleo puro).** `Application.rebalance_plan/2` faz o diff do
    placement **atual** × `desired_placement` (R1) por vnode: para cada vnode cujo conjunto de nós difere,
    devolve `%{vnode_id:, add:, remove:}` (nós a entrar / a sair do cluster `ra`); vnodes já corretos são
    omitidos (plano vazio = nada a fazer). *Staged/planned* — computa sem aplicar. A ordem segura é
    **add-before-remove** (o R3 adiciona antes de remover, então um vnode nunca cai abaixo do quórum no
    meio da mudança; com RF constante, `add` e `remove` têm o mesmo tamanho). Assume o **mesmo conjunto de
    vnode ids** em atual e desejado (mudar a contagem é re-sharding, fora de escopo). Determinístico
    (segue a ordem do desejado). Testado: vazio quando nada muda; add/remove por vnode alterado (omite os
    iguais); num *join* o RF fica constante (add/remove equilibrados → nunca abaixo do quórum); num
    *leave* só os vnodes que detinham o nó que saiu entram no plano e nenhum re-adiciona o nó removido.
  - **R0 — Lease sobre `ra`** (fencing forte, k8s): pré-requisito de R3 (o movimento de vnodes é
    **não-idempotente** — é aqui que o lease finalmente entra, como antecipado no 1C-b).
    - ✅ **R0-a — máquina de estado do lease (núcleo puro + `ra`).** `Malachi.Cluster.Lease` é o estado
      puro (`holder`, `fence`, `renew_at`, `duration_ms`): `acquire_or_renew` concede se **livre**, **já é
      o holder** (renovação) ou **expirado** (`now >= renew_at + duration_ms`), senão `{:error, {:held,
      holder}}`; `release` é idempotente. O **fencing token** (`fence`) é monotônico e sobe **só quando o
      holder muda** (renovação mantém) — o holder o carrega para o trabalho que fencia, e uma escrita de
      ex-holder com token obsoleto pode ser rejeitada (a proteção contra dois chefes). O tempo (`now`) é
      **injetado**, nunca lido dentro do `apply` (seria não-determinístico e quebraria o Raft):
      `LeaseMachine` (`:ra_machine`) alimenta o `meta.system_time` do `ra` (relógio do **líder**,
      carimbado uma vez e replicado no log), então um único relógio decide a expiração — sem o skew
      entre nós que um tempo vindo do cliente carregaria. `LeaseServer` espelha o `MetadataServer` sobre
      um cluster `ra` **dedicado** (isolado do metadata). Testado: `Lease` puro exaustivo (aquisição/
      renovação mantém fence/roubo na expiração incrementa fence/fronteira exata do deadline/release
      idempotente com token obsoleto) + integração `ra` real (acquire/renew/held/release/durável a restart).
    - ✅ **R0-b — `LeaseHolder` (o client).** GenServer que roda o triângulo de timers `duração >
      renew_deadline_ms > retry_period_ms`: a cada `retry_period` chama o seam `renew` (acquire-or-renew);
      um *follower* que adquire vira *leader* e chama `on_acquired(fence)`; um *leader* que renova segue
      líder (marca o instante do renew no **relógio local**); se lhe dizem que o lease está com **outro**
      larga na hora (`on_lost`); se **não alcança** o lease, segue tentando até passar `renew_deadline_ms`
      desde o último renew bem-sucedido e então **larga proativo** (`on_lost`, o *OnStoppedLeading* do k8s
      — desiste antes de o lease poder expirar/ser roubado, para nunca haver dois líderes). Um salto do
      **fencing token** durante a liderança (gap: perdeu e reganhou) dispara `on_lost` seguido de
      `on_acquired` sob o novo token. No shutdown normal, um líder **libera** o lease (failover sem
      esperar expiração). Tudo por **seams injetados** (`renew`/`release`/`clock`/callbacks), então a
      lógica de tempo é testada sem `ra`, controlando o relógio. Testado: adquire→líder; renova sem
      re-`on_acquired`; segura até o deadline e larga; largada imediata quando held-por-outro; troca de
      token → lost+acquired; release no shutdown do líder (e não do follower). **R0 completo.**
  - **R3 — execução** (*committed*): aplica o plano por vnode via `ra`, **sob o lease**. Escopo: control
    plane (o `ra` transfere o estado ao adicionar membro); o data plane fica com o *healing*.
    - ✅ **R3-a — executor de uma mudança (núcleo com seams).** `Malachi.Cluster.Rebalance`: `apply_change/3`
      aplica cada `add` **antes** de cada `remove` (add-before-remove, para o vnode nunca cair abaixo do
      quórum) via seams `add_member`/`remove_member` (`(vnode_id, node -> :ok | {:error, _})`); **idempotente**
      (add de quem já é membro / remove de quem já saiu = `:ok`, então um commit interrompido é
      re-executável); **fail-fast** (um `add` que falha **não** tenta os removes — protege o quórum).
      `apply_plan/4` aplica o plano mudança-a-mudança, fail-fast entre vnodes (para no 1º erro, devolve
      `{:error, {aplicados, falha}}`), e **revalida `leader?` antes de cada mudança** (para com
      `:lost_leadership` se o holder soltou o lease no meio). Testado (seams que gravam a ordem): add
      antes de remove; add que falha não remove; erro no remove reportado; idempotência; plano completo;
      fail-fast entre vnodes; parada por perda de liderança.
    - **R3-b — coordenador plan/commit + ops `ra`/wiring.**
      - ✅ **R3-b-i — plano do estado vivo (núcleo com seams).** `Application.readable_placement/2` monta o
        placement **atual** a partir das memberships `ra` dos vnodes via o seam `members_of`
        (`vnode_id -> {:ok, nodes} | {:error, _}`), **omitindo** vnodes ilegíveis (conservador: nunca
        planejar um vnode que não conseguimos ver). `Application.live_rebalance_plan/5` é o *plan*: faz o
        diff do atual (legível) contra o `place_vnodes` **desejado** sobre os nós **vivos**, só para os
        vnodes legíveis, e devolve o plano (`rebalance_plan/2`) que alimenta `Rebalance.apply_plan/4` (o
        *commit*, sob o lease). Fica junto de R1/R2 no `Application` (evita ciclo com `Rebalance`, que só
        executa). Testado: `readable_placement` omite ilegível; `live_rebalance_plan` = diff atual×desejado
        sobre vivos; vazio quando já casa; nunca planeja vnode ilegível.
      - ✅ **R3-b-ii — ops `ra` reais + coordenador (motor).** `Rebalance.ra_add_member/3` e
        `ra_remove_member/3` são os seams reais do `apply_plan`: **add** = `add_member` (anuncia o membro —
        ele nem precisa estar rodando) **depois** `start_server` no nó via `:erpc` (a ordem que a doc do
        `ra` prescreve; o líder então replica log/snapshot ao novo membro); **remove** = `remove_member`
        depois `stop_server`. **Idempotentes** (`already_member`/`not_member`/`already_started` aninhado →
        `:ok`) e com **retry** em `:cluster_change_not_permitted` — o `ra` só permite **uma** mudança de
        membership por vez, então o `add`-then-`remove` de um mesmo change (e ops repetidas) esperam a
        anterior assentar. `RebalanceCoordinator` (GenServer, seams `plan_fun`/`add_member`/`remove_member`/
        `leader?`) expõe `plan/1` (calcula, não aplica) e `commit/1` (**recusa `:not_leader`** se não for o
        holder do lease; senão `apply_plan` fail-fast, passando o mesmo `leader?` para parar se o lease cair
        no meio). Commit **sempre manual**. Testado: coordenador por seams (plan/commit/recusa/fail-fast) +
        **`:multinode`** real — `ra_add_member` cresce um vnode para um novo nó e o `ra` transfere o estado,
        `ra_remove_member` encolhe, ambos idempotentes.
      - ✅ **R3-b-iii — wiring (\"ligar na tomada\").** Quando **sharded**, `Application.log_children`
        adiciona `rebalance_children`: bootstrapa o `LeaseServer` (cluster `ra` dedicado `Malachi.LogLease`,
        **auto-fencido** no boot — todo nó chama, um forma) e sobe o `LeaseHolder` (`Malachi.LogLeaseHolder`,
        triângulo default 15s/10s/2s via `lease_duration_ms`/`lease_renew_deadline_ms`/`lease_retry_period_ms`,
        `renew`/`release` reais sobre o `LeaseServer`) e o `RebalanceCoordinator` (`Malachi.LogRebalanceCoordinator`)
        com os seams reais: `plan_fun`=`live_rebalance_plan`, `add_member`/`remove_member`=`Rebalance.ra_*`
        resolvendo os membros via `try_members/2` (tenta cada nó como ponto de entrada — o holder pode não
        hospedar o vnode; qualquer membro roteia ao líder), `leader?`=`LeaseHolder.leader?`. Adicionei
        `LeaseHolder.leader?/1` (lê o papel sem forçar tick). O **commit segue manual** (o operador chama
        `RebalanceCoordinator.plan/1`/`commit/1`); o `LeaseHolder` só mantém a eleição rodando (k8s). O
        caminho **não-sharded é inalterado**. Testado: `leader?/1` e `try_members/2` isolados + suíte
        completa (1048 testes) verde = boot não regride; comportamento das ops apoiado no `:multinode` de
        R3-b-ii. **Rebalancing dinâmico completo — a Fase 1 (distribuição) fecha aqui.**
      - ✅ **Reconcile do lease (endurecimento).** O bootstrap `auto-fencido` acima forma o cluster do
        lease só com a **maioria** (Raft); um nó que estava down quando o cluster formou fica membro da
        **config** (o `start_cluster` inicial lista todos os nós) mas sem servidor rodando, reduzindo a
        tolerância a falha do lease. `LeaseServer.reconcile/2` faz **self-join** — best-effort e idempotente:
        (re)tenta formar o cluster (`start/2`, auto-fencido) e iniciar o servidor **local** (`:ra.start_server`),
        que se re-junta ao cluster existente (já é membro da config) e o `ra` replica o estado do lease a
        ele. `Malachi.Cluster.LeaseReconciler` (GenServer genérico, seam `:reconcile`) o chama após o boot
        e a cada `lease_reconcile_interval_ms` (default 30s), *level-triggered* — mantém o `LeaseHolder`
        livre de `ra`/membership. Subido no `rebalance_children` (primeiro, antes do holder). Testado:
        reconcile bootstrapa quando não iniciado + é no-op idempotente num cluster formado (não perturba o
        lease); o reconciler reconcilia no boot e sob demanda.

> A ordem de execução das fatias restantes (`place_vnodes` A2 ✅; camada B do cliente) é decidida quando
> cada uma for atacada.
