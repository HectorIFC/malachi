# Malachi — Ideias de vídeos animados (estilo Veritasium)

> Vídeos curtos, didáticos, com analogias fortes. Cada roteiro indica **[NAR]** narração e **[VIS]** o que aparece na tela.
> Formato sugerido por vídeo indicado em cada card (9:16 vertical para Shorts/Reels/TikTok, 16:9 horizontal para YouTube).
> Repo: **https://github.com/HectorIFC/malachi**

---

## O que é o Malachi (resumo para embasar os vídeos)

Malachi é um **log broker** open-source, 100% Elixir, que reimplementa a arquitetura **NorthGuard** do LinkedIn. É **CP** (consistente e tolerante a partição), replicado por quórum (Raft via `ra`), com membership por SWIM, self-healing e placement ciente de rack/datacenter. Concorrente direto do **Kafka**.

**Pontos fortes vs. concorrentes (principalmente Kafka):**

1. **Cursor opaco** — o cliente fala em *tópico, chave e cursor*, nunca em partições ou offsets. O broker pode dividir, unir e re-listrar (restripe) o armazenamento por baixo **sem quebrar o cliente**. Este é o diferencial nº 1: o Kafka *vaza* partições e offsets para o cliente.
2. **Ranges dinâmicos** — fatias do keyspace que se **dividem** conforme crescem (estilo buddy-allocator); split/merge são operações **puramente lógicas de metadados** (não copiam dados).
3. **Segmento como unidade de replicação** — replicado por quórum entre nós.
4. **Sem SPOF** — control plane multi-nó em Raft + data plane com replicação por quórum, failover, catch-up e membership SWIM.
5. **Auto-balanceamento e re-sharding** — vnode split sobre Raft, heal de segmentos sub-replicados, resharding para crescer.
6. **BEAM/Elixir** — concorrência massiva e tolerância a falhas nativas; entrega 10–50x o throughput-alvo do NorthGuard num laptop.
7. **Backpressure real** — controle de fluxo por janela de crédito; consumidor lento aplica contrapressão em vez de estourar memória.
8. **Segurança madura** — TLS 1.2/1.3 + mTLS, Argon2, rate limiting, lockout progressivo, prevenção de exaustão de atoms, auditoria.

**Edge cases interessantes (bom material de vídeo):**
Migration fence (seal-first) durante split para não perder escrita concorrente; migração copy-first (nenhuma falha isolada perde um tópico); offsets do consumer group preservados no split; leituras cross-epoch (histórico de uma chave atravessando um split); 4 estratégias de overflow; placement `hard`/`soft` com `min_domains`; retry de cliente em `:migrating`/`:not_owner`.

---

# BLOCO 1 — Os diferenciais fundamentais

Vídeos que explicam *por que o Malachi existe* e onde ele ganha do Kafka. Ideais para 9:16 (ganchos fortes, conceito único por vídeo).

---

## 🎬 Vídeo 1 — "O Kafka te obriga a saber demais"

**Formato:** 9:16 vertical · ~60s

**Descrição da capa:** Fundo escuro. À esquerda, um mapa cheio de coordenadas, números e setas confusas rotulado "KAFKA". À direita, um único cartão limpo com uma seta simples rotulado "MALACHI". Texto grande: **"Você não deveria saber onde seus dados moram."**

**Roteiro:**
- [NAR] Imagine estacionar o carro num shopping. Duas opções.
- [VIS] Split de tela. Esquerda: pessoa anotando "Piso 3, vaga G-47, setor azul".
- [NAR] Na primeira, você precisa decorar o andar, o setor e o número exato da vaga. Se o shopping reorganizar as vagas amanhã... você se perde.
- [VIS] As vagas se reorganizam; a anotação vira um "❌".
- [NAR] Na segunda, você entrega a chave e recebe um **ticket**. Não sabe onde o carro foi parar — e não precisa.
- [VIS] Direita: valet entrega um ticket. Carro some por uma porta; ticket brilha.
- [NAR] O Kafka é o primeiro estacionamento: ele te obriga a saber a *partição* e o *offset* exatos.
- [VIS] "Partição 4, offset 91422" pisca em vermelho.
- [NAR] O Malachi é o valet. Você guarda só um **cursor opaco** — um ticket. Por baixo, o broker pode dividir, juntar e remanejar tudo...
- [VIS] Blocos de dados se dividem e se reorganizam animadamente.
- [NAR] ...e o seu ticket continua valendo. Você nunca precisou saber onde os dados moravam.
- [VIS] Ticket "MALACHI" continua verde enquanto o fundo se reorganiza.

**CTA:** [VIS/NAR] "É open-source e feito em Elixir. Código no GitHub: **github.com/HectorIFC/malachi** — deixe uma ⭐."

---

## 🎬 Vídeo 2 — "Por que trocar de vaga quebra o Kafka (e não quebra o Malachi)"

**Formato:** 9:16 vertical · ~75s

**Descrição da capa:** Uma célula se dividindo em duas (mitose), estilo biologia, com o rótulo **"Seus dados cresceram. E agora?"**. Cores vivas sobre fundo escuro.

**Roteiro:**
- [NAR] Todo sistema de dados enfrenta o mesmo problema: e quando um pedaço cresce demais?
- [VIS] Uma barra de dados incha até quase estourar.
- [NAR] No Kafka, você escolhe o número de partições *na frente*. Errou pra menos? Aumentar depois embaralha para onde cada chave vai — e pode bagunçar a ordem.
- [VIS] Chaves pulando de caixa para caixa, uma bandeira "ordem?" tremulando.
- [NAR] O Malachi trata isso como uma **célula que se divide**. Um "range" — uma fatia do espaço de chaves — cresce e simplesmente se parte em dois.
- [VIS] A célula/range se divide em duas metades limpas (estilo buddy: uma barra de chocolate partida ao meio).
- [NAR] E aqui está o truque: essa divisão **não copia nenhum dado**. É só uma anotação nos metadados. Instantânea.
- [VIS] Um lápis desenha uma linha divisória; os dados ficam parados, só a "etiqueta" muda.
- [NAR] O cliente? Nem percebe. O cursor dele continua funcionando, porque ele nunca soube em qual metade estava.
- [VIS] Ticket do vídeo anterior reaparece, verde, atravessando a divisão.
- [NAR] Crescer deixa de ser uma migração assustadora. Vira rotina.

**CTA:** [VIS/NAR] "Quer ver como o split funciona no código? **github.com/HectorIFC/malachi**"

---

## 🎬 Vídeo 3 — "Como computadores 'votam' para nunca perder seus dados"

**Formato:** 9:16 vertical · ~70s

**Descrição da capa:** Cinco robôs idênticos levantando a mão; três estão iluminados. Texto: **"A maioria decide. Sempre."** Subtítulo pequeno: "Replicação por quórum".

**Roteiro:**
- [NAR] Você mandou uma mensagem. Como o sistema garante que ela não vai sumir se um servidor pegar fogo?
- [VIS] Um servidor solitário com uma chama; a mensagem evapora. "😱"
- [NAR] A resposta do Malachi: nunca confie num servidor só. Confie na **maioria**.
- [VIS] Aparecem 3 servidores lado a lado.
- [NAR] Quando um dado chega, ele é replicado. E só é considerado "salvo" quando a **maioria** confirma: recebi, guardei.
- [VIS] A mensagem se copia para os 3; dois deles dão "✅"; um selo "COMMITTED" aparece.
- [NAR] Isso se chama quórum. Com três cópias, você pode perder uma inteira...
- [VIS] Um servidor explode; os outros dois seguem firmes.
- [NAR] ...e nada é perdido. Os dois que sobraram já eram maioria. É o algoritmo Raft, o mesmo princípio por trás de bancos de dados sérios.
- [VIS] Um novo servidor "nasce" e se sincroniza com os outros dois (self-healing).
- [NAR] E o Malachi ainda conserta sozinho: sobe uma nova cópia para voltar a ter três. Ninguém precisa acordar de madrugada.

**CTA:** [VIS/NAR] "Raft, quórum e self-healing, tudo em Elixir open-source: **github.com/HectorIFC/malachi**"

---

## 🎬 Vídeo 4 — "O sistema que fofoca para se manter vivo"

**Formato:** 9:16 vertical · ~60s

**Descrição da capa:** Vários personagens numa festa com balões de fala conectados por linhas, um deles apagado/cinza. Texto: **"Como servidores descobrem que um amigo caiu — fofocando."** (protocolo SWIM).

**Roteiro:**
- [NAR] Numa festa com 100 pessoas, como todo mundo descobre que alguém foi embora? Ninguém faz uma chamada. A notícia **se espalha**.
- [VIS] Festa animada; uma pessoa sai; o vizinho cochicha; o cochicho se propaga em ondas.
- [NAR] Servidores num cluster têm o mesmo desafio. O Malachi usa um protocolo de "fofoca" chamado SWIM.
- [VIS] Nós como convidados. Um cutuca o outro: "você tá vivo?".
- [NAR] Cada nó, de vez em quando, cutuca outro ao acaso: "responde aí". Sem resposta? Ele pergunta a alguns amigos para confirmar...
- [VIS] Nó silencioso fica cinza; três vizinhos tentam confirmar antes de marcar como "caiu".
- [NAR] ...e só então espalha a notícia. Nada de um chefe central vigiando todo mundo — isso seria um ponto único de falha.
- [VIS] A informação se espalha organicamente; nenhum "servidor-chefe" no centro.
- [NAR] O resultado: o cluster inteiro concorda sobre quem está vivo, gastando quase nada de rede.

**CTA:** [VIS/NAR] "Membership, gossip e zero ponto único de falha: **github.com/HectorIFC/malachi**"

---

## 🎬 Vídeo 5 — "Um formigueiro com 1 milhão de trabalhadores (por que Elixir?)"

**Formato:** 16:9 horizontal · ~90s (conceito mais "explicativo", cabe bem na horizontal)

**Descrição da capa:** Um formigueiro estilizado visto em corte, com milhares de formigas carregando pacotinhos de dados. Texto: **"Por que construir um broker em Elixir?"**

**Roteiro:**
- [NAR] A maioria dos softwares é como um único trabalhador super-forte: rápido, mas se ele tropeça, tudo para.
- [VIS] Um operário gigante levantando uma carga; ele cai; tudo congela.
- [NAR] O Elixir, sobre a máquina virtual BEAM, funciona diferente. Em vez de um herói, ele usa **milhões de formigas minúsculas** — processos independentes e baratíssimos.
- [VIS] O operário se dissolve em milhares de formigas, cada uma com um pacotinho.
- [NAR] Cada conexão, cada tópico, cada tarefa é uma formiga. Se uma tropeça...
- [VIS] Uma formiga cai; uma "supervisora" imediatamente coloca outra no lugar.
- [NAR] ...uma supervisora coloca outra no lugar em milissegundos. O formigueiro nem sente. Isso se chama tolerância a falhas, e é nativo aqui.
- [VIS] O fluxo de formigas segue ininterrupto.
- [NAR] Para um broker de mensagens — que precisa ficar de pé o tempo todo e lidar com milhares de conexões ao mesmo tempo — é o material perfeito.
- [VIS] Números aparecem: "10–50x o throughput-alvo, num laptop".
- [NAR] O Malachi aproveita isso: a metade difícil de um sistema distribuído, a BEAM já resolve de fábrica.

**CTA:** [VIS/NAR] "100% Elixir, open-source: **github.com/HectorIFC/malachi**"

---

# BLOCO 2 — Internals e edge cases

Vídeos "nível 2", para quem já pegou o conceito. Mostram a engenharia fina — ótimos para retenção de público técnico.

---

## 🎬 Vídeo 6 — "Bebendo de um hidrante: o problema do consumidor lento"

**Formato:** 9:16 vertical · ~65s

**Descrição da capa:** Uma pessoa tentando beber de um hidrante aberto, água jorrando por todo lado. Texto: **"E se quem recebe for mais lento que quem envia?"** (backpressure).

**Roteiro:**
- [NAR] Um produtor dispara mil mensagens por segundo. O consumidor só dá conta de cem. O que acontece com as outras 900?
- [VIS] Cano grosso jorrando em cano fino; água transbordando.
- [NAR] Num sistema ingênuo, elas se acumulam na memória até... o servidor estourar.
- [VIS] Um balde enchendo até explodir "💥 OUT OF MEMORY".
- [NAR] O Malachi resolve com **contrapressão**. O consumidor emite "créditos": só mande o que eu consigo processar agora.
- [VIS] Consumidor segura plaquinhas "posso receber 100"; produtor respeita.
- [NAR] Ficou lento? Ele simplesmente pede menos, e a pressão volta pela linha até o produtor desacelerar.
- [VIS] Uma válvula fecha suavemente; o fluxo se ajusta em vez de transbordar.
- [NAR] E se a fila encher mesmo assim, você escolhe a política: descartar o mais novo, o mais velho, rejeitar, ou bloquear e esperar.
- [VIS] Quatro botões: "drop newest / drop oldest / reject / block".
- [NAR] Nada de estourar silenciosamente. O sistema degrada com elegância.

**CTA:** [VIS/NAR] "Backpressure e 4 estratégias de overflow, documentadas: **github.com/HectorIFC/malachi**"

---

## 🎬 Vídeo 7 — "Como mudar um trem de trilho sem descarrilar (o split ao vivo)"

**Formato:** 16:9 horizontal · ~90s

**Descrição da capa:** Um trilho de trem se bifurcando, um trem passando exatamente no ponto da troca, tudo suave. Texto: **"Reorganizar o cluster — sem perder uma única mensagem."**

**Roteiro:**
- [NAR] Dividir um pedaço do cluster enquanto ele recebe escritas é como trocar o trilho de um trem em movimento. Faça errado e você perde carga.
- [VIS] Trem em alta velocidade; um trilho começa a se bifurcar à frente.
- [NAR] O perigo: no meio da mudança, chega uma escrita nova. Para onde ela vai? Se ninguém decidir, ela some.
- [VIS] Um pacote de dados chega no exato momento da troca, piscando "?".
- [NAR] A solução do Malachi é **selar antes de mover**. Ele "lacra" o tópico que está migrando: novas escritas ficam em espera com um aviso claro — "estou migrando".
- [VIS] Um selo de cera fecha um envelope "R1"; escritas batem na porta e recebem "migrating, aguarde".
- [NAR] Só então ele copia os metadados para o novo grupo — **copiar primeiro, remover depois**.
- [VIS] Envelope é duplicado para o novo lado; só depois o original é apagado.
- [NAR] Por que copiar primeiro? Porque se algo falhar no meio, no pior caso sobra uma cópia duplicada e inofensiva — nunca um tópico perdido.
- [VIS] Uma falha "💥" no meio; a cópia duplicada continua lá, marcada como segura.
- [NAR] E o cliente? Ele recebe "migrating", espera um instante e tenta de novo — automaticamente. Nem o consumidor perde o lugar: a posição dele viaja junto na migração.
- [VIS] Cliente com um "~" tentando de novo; barra de progresso do consumer chega intacta do outro lado.
- [NAR] Trem trocou de trilho. Ninguém a bordo sentiu.

**CTA:** [VIS/NAR] "Vnode split sobre Raft, fence e copy-first, no código: **github.com/HectorIFC/malachi**"

---

## 🎬 Vídeo 8 — "A máquina do tempo de uma chave (leitura cross-epoch)"

**Formato:** 9:16 vertical · ~70s

**Descrição da capa:** Uma linha do tempo de uma "chave" com uma bifurcação no meio (um split), e uma lupa seguindo o rastro por cima da bifurcação. Texto: **"O histórico não pode quebrar quando os dados se dividem."**

**Roteiro:**
- [NAR] Lembra que os ranges se dividem como células? Isso cria um problema sutil.
- [VIS] Range se divide em dois (callback do Vídeo 2).
- [NAR] O histórico da chave "usuário-42" começou *antes* da divisão e continuou *depois*, em outra metade. Como ler a história inteira sem furos?
- [VIS] Uma trilha de migalhas que atravessa a linha da divisão.
- [NAR] O Malachi guarda a **linhagem**: cada range sabe quem foi seu "pai".
- [VIS] Setas de parentesco ligando range-filho ao range-pai, como uma árvore genealógica.
- [NAR] Para reconstruir o histórico, ele segue a árvore genealógica para trás, respeitando a ordem "aconteceu-antes".
- [VIS] Lupa percorre do filho para o pai, montando a timeline em ordem.
- [NAR] Resultado: você lê a vida inteira de uma chave, atravessando quantas divisões existirem, como se nada tivesse mudado.
- [VIS] Timeline contínua e completa se ilumina de ponta a ponta.

**CTA:** [VIS/NAR] "Leituras cross-epoch e linhagem de ranges: **github.com/HectorIFC/malachi**"

---

## 🎬 Vídeo 9 — "Não coloque todos os ovos na mesma cesta (placement ciente de rack)"

**Formato:** 9:16 vertical · ~60s

**Descrição da capa:** Três cestas de ovos em prateleiras diferentes; uma prateleira desabando, mas os ovos das outras intactos. Texto: **"3 cópias no mesmo rack não são 3 cópias."**

**Roteiro:**
- [NAR] Você replicou seu dado três vezes. Seguro, né? Depende de *onde* essas cópias estão.
- [VIS] Três ovos... todos na mesma cesta.
- [NAR] Se as três cópias moram no mesmo rack, e o rack cai, você perde as três de uma vez. Três cópias, um único ponto de falha.
- [VIS] A prateleira desaba; as três se quebram juntas. "❌❌❌"
- [NAR] O Malachi entende de "domínios de falha". Você marca cada servidor com atributos — rack, datacenter — e ele espalha as cópias por domínios diferentes.
- [VIS] Os três ovos se distribuem em três prateleiras separadas.
- [NAR] E você escolhe o rigor: no modo **soft**, ele faz o melhor possível; no modo **hard**, ele *recusa* a escrita se não conseguir espalhar o suficiente.
- [VIS] Dois seletores: "soft: melhor esforço" e "hard: exige separação".
- [NAR] Cai um rack inteiro? As outras cópias estão sãs e salvas, longe dali.
- [VIS] Uma prateleira desaba; as outras duas seguem intactas; selo "seguro".

**CTA:** [VIS/NAR] "Placement por atributos, rack/DC-aware: **github.com/HectorIFC/malachi**"

---

## 🎬 Vídeo 10 — "O teorema que te obriga a escolher (CAP: por que o Malachi é CP)"

**Formato:** 16:9 horizontal · ~85s

**Descrição da capa:** Um triângulo com os vértices "Consistência", "Disponibilidade" e "Partição"; dois vértices acesos. Texto: **"Você não pode ter os três. O Malachi escolhe."**

**Roteiro:**
- [NAR] Todo sistema distribuído esbarra numa lei implacável: o teorema CAP.
- [VIS] Triângulo aparece: Consistência, Disponibilidade, tolerância a Partição.
- [NAR] Quando a rede se parte em dois — e ela *vai* se partir — você é obrigado a escolher.
- [VIS] Um cabo de rede se rompe; o cluster vira dois grupos isolados.
- [NAR] Opção A: continuar respondendo dos dois lados. Rápido, sempre no ar... mas os dois lados podem discordar. Você lê dados errados.
- [VIS] Dois lados dando respostas diferentes para a mesma pergunta; "🤔 qual é a verdade?".
- [NAR] Opção B: recusar responder do lado que não tem a maioria, até a rede voltar. Menos disponível, mas **nunca mente**.
- [VIS] O lado minoritário levanta a mão "não sei, prefiro não responder"; o lado da maioria segue confiável.
- [NAR] O Malachi é o time B: **consistente e tolerante a partição**. Ele prefere pausar a te dar uma resposta errada.
- [VIS] Selo "CP" acende sobre o triângulo.
- [NAR] Para logs, mensagens financeiras, eventos que não podem divergir — é exatamente a escolha certa.

**CTA:** [VIS/NAR] "Um broker CP feito em Elixir: **github.com/HectorIFC/malachi**"

---

## 🎬 Vídeo 11 (bônus) — "O cadeado que fica mais lento a cada tentativa (segurança)"

**Formato:** 9:16 vertical · ~55s

**Descrição da capa:** Um cadeado que vai ficando maior e mais pesado a cada tentativa errada de um invasor. Texto: **"Errou a senha? A porta fica mais difícil."** (lockout progressivo).

**Roteiro:**
- [NAR] Um invasor tenta adivinhar sua senha, milhares de vezes por segundo. Como parar isso sem incomodar quem é legítimo?
- [VIS] Robô martelando senhas numa fechadura.
- [NAR] O Malachi usa **lockout progressivo**. Cada tentativa errada torna a próxima mais lenta.
- [VIS] A cada erro, um cadeado engorda e um cronômetro cresce: 5 min, 10, 20...
- [NAR] Para um humano que errou a senha, é um segundo de espera. Para um robô tentando um milhão de combinações, vira uma eternidade.
- [VIS] Robô travado, cronômetro gigante; usuário real entra tranquilo em seguida.
- [NAR] Some a isso senhas guardadas com Argon2, TLS obrigatório em produção e limites de conexão por IP...
- [VIS] Ícones: 🔒 Argon2 · 🔐 TLS · 🚧 rate limit.
- [NAR] ...e você tem um broker que não foi seguro por acidente. Foi projetado assim.

**CTA:** [VIS/NAR] "Endurecimento de segurança completo, documentado: **github.com/HectorIFC/malachi**"

---

# Como usar esta lista

- **Sequência recomendada de publicação:** comece pelos Vídeos 1 e 2 (o gancho do cursor opaco é o diferencial mais vendável e o mais fácil de entender). Depois 3, 4, 5 para fundar os conceitos distribuídos. O Bloco 2 vem em seguida, para reter o público técnico.
- **Reaproveitamento de assets:** o "ticket/valet" (V1), a "célula/range" (V2) e os "3 servidores votando" (V3) viram uma linguagem visual recorrente — reaparecem nos vídeos de internals como callbacks (já sinalizados nos roteiros).
- **CTA padronizado:** todos terminam apontando para **github.com/HectorIFC/malachi** com pedido de ⭐. Mantê-lo idêntico ajuda no reconhecimento entre vídeos.
</content>
</invoke>
