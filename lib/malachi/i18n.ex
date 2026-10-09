defmodule Malachi.I18n do
  @moduledoc """
  Internationalization module for Malachi.
  Supports Brazilian Portuguese (pt_BR) and American English (en_US).

  ## Configuration

      config :malachi, :locale, "pt_BR"  # or "en_US"

  ## Usage

      Malachi.I18n.t(:metrics_started)
      Malachi.I18n.t(:transport_enabled, transport: "TLS", port: 4040)
  """

  @translations %{
    tcp_server_started: %{
      "pt_BR" => "🚀 Malachi TCP Server na porta %{port} com %{acceptors} acceptors",
      "en_US" => "🚀 Malachi TCP Server on port %{port} with %{acceptors} acceptors"
    },
    transport_enabled: %{
      "pt_BR" => "🔒 Transporte %{transport} habilitado na porta %{port}",
      "en_US" => "🔒 %{transport} transport enabled on port %{port}"
    },
    tls_handshake_failed: %{
      "pt_BR" => "Falha no handshake TLS: %{reason}",
      "en_US" => "TLS handshake failed: %{reason}"
    },
    # TLS Validator translations
    tls_validation_started: %{
      "pt_BR" => "🔒 Iniciando validação TLS...",
      "en_US" => "🔒 Starting TLS validation..."
    },
    tls_validation_success: %{
      "pt_BR" => "✅ Validação TLS concluída com sucesso",
      "en_US" => "✅ TLS validation completed successfully"
    },
    tls_validation_failed: %{
      "pt_BR" => "❌ Falha na validação TLS: %{reason}",
      "en_US" => "❌ TLS validation failed: %{reason}"
    },
    tls_required_but_disabled: %{
      "pt_BR" =>
        "═══════════════════════════════════════════════════════════════\nERRO DE SEGURANÇA: TLS é obrigatório em produção\n═══════════════════════════════════════════════════════════════\n\nConfigure certificados TLS:\n  MALACHI_TLS_CERTFILE=/caminho/para/certificado.pem\n  MALACHI_TLS_KEYFILE=/caminho/para/chave_privada.pem\n  MALACHI_ENABLE_TLS=true\n\nOu desabilite a exigência de TLS (NÃO RECOMENDADO):\n  MALACHI_REQUIRE_TLS=false\n═══════════════════════════════════════════════════════════════",
      "en_US" =>
        "═══════════════════════════════════════════════════════════════\nSECURITY ERROR: TLS is required in production\n═══════════════════════════════════════════════════════════════\n\nConfigure TLS certificates:\n  MALACHI_TLS_CERTFILE=/path/to/certificate.pem\n  MALACHI_TLS_KEYFILE=/path/to/private_key.pem\n  MALACHI_ENABLE_TLS=true\n\nOr disable TLS requirement (NOT RECOMMENDED):\n  MALACHI_REQUIRE_TLS=false\n═══════════════════════════════════════════════════════════════"
    },
    tls_cert_file_not_found: %{
      "pt_BR" => "Arquivo de certificado TLS não encontrado: %{path}",
      "en_US" => "TLS certificate file not found: %{path}"
    },
    tls_cert_file_unreadable: %{
      "pt_BR" => "Arquivo de certificado TLS não pode ser lido: %{path} (%{reason})",
      "en_US" => "TLS certificate file cannot be read: %{path} (%{reason})"
    },
    tls_key_file_not_found: %{
      "pt_BR" => "Arquivo de chave privada TLS não encontrado: %{path}",
      "en_US" => "TLS private key file not found: %{path}"
    },
    tls_key_file_unreadable: %{
      "pt_BR" => "Arquivo de chave privada TLS não pode ser lido: %{path} (%{reason})",
      "en_US" => "TLS private key file cannot be read: %{path} (%{reason})"
    },
    tls_cert_expired: %{
      "pt_BR" => "Certificado TLS EXPIRADO em %{expiry_date}",
      "en_US" => "TLS certificate EXPIRED on %{expiry_date}"
    },
    tls_cert_expiring_soon: %{
      "pt_BR" => "⚠️ Certificado TLS expira em %{days} dias (%{expiry_date})",
      "en_US" => "⚠️ TLS certificate expires in %{days} days (%{expiry_date})"
    },
    tls_cert_valid: %{
      "pt_BR" => "Certificado TLS válido até %{expiry_date} (%{days} dias restantes)",
      "en_US" => "TLS certificate valid until %{expiry_date} (%{days} days remaining)"
    },
    tls_cert_not_yet_valid: %{
      "pt_BR" => "Certificado TLS ainda não é válido (início: %{not_before})",
      "en_US" => "TLS certificate not yet valid (starts: %{not_before})"
    },
    tls_cert_empty: %{
      "pt_BR" => "Arquivo de certificado TLS está vazio: %{path}",
      "en_US" => "TLS certificate file is empty: %{path}"
    },
    tls_cert_wrong_format: %{
      "pt_BR" => "Certificado TLS em formato inválido (esperado PEM): %{path}",
      "en_US" => "TLS certificate in invalid format (expected PEM): %{path}"
    },
    tls_key_cert_mismatch: %{
      "pt_BR" => "Chave privada TLS não corresponde ao certificado",
      "en_US" => "TLS private key does not match certificate"
    },
    tls_weak_key_size: %{
      "pt_BR" => "⚠️ Chave TLS com tamanho fraco: %{size} bits (mínimo recomendado: %{min_size} bits)",
      "en_US" => "⚠️ Weak TLS key size: %{size} bits (minimum recommended: %{min_size} bits)"
    },
    tls_unsupported_version: %{
      "pt_BR" => "Versão TLS insegura configurada: %{version}. Use apenas TLS 1.2 ou 1.3.",
      "en_US" => "Insecure TLS version configured: %{version}. Use TLS 1.2 or 1.3 only."
    },
    tls_key_world_readable: %{
      "pt_BR" => "⚠️ Chave privada TLS tem permissões muito abertas: %{path}",
      "en_US" => "⚠️ TLS private key has overly permissive permissions: %{path}"
    },
    tls_versions_configured: %{
      "pt_BR" => "Versões TLS configuradas: %{versions}",
      "en_US" => "TLS versions configured: %{versions}"
    },
    tls_cert_not_configured: %{
      "pt_BR" => "Arquivo de certificado TLS não configurado (MALACHI_TLS_CERTFILE)",
      "en_US" => "TLS certificate file not configured (MALACHI_TLS_CERTFILE)"
    },
    tls_key_not_configured: %{
      "pt_BR" => "Arquivo de chave privada TLS não configurado (MALACHI_TLS_KEYFILE)",
      "en_US" => "TLS private key file not configured (MALACHI_TLS_KEYFILE)"
    },
    acceptor_started: %{
      "pt_BR" => "Acceptor #%{id} iniciado",
      "en_US" => "Acceptor #%{id} started"
    },
    accept_error: %{
      "pt_BR" => "Erro no accept: %{reason}",
      "en_US" => "Accept error: %{reason}"
    },
    metrics_started: %{
      "pt_BR" => "✅ Sistema de métricas iniciado",
      "en_US" => "✅ Metrics system started"
    },
    dashboard_started: %{
      "pt_BR" => "🌐 Malachi Dashboard rodando em http://localhost:%{port}",
      "en_US" => "🌐 Malachi Dashboard running at http://localhost:%{port}"
    },
    console_started: %{
      "pt_BR" => "Console do Malachi servindo em http://localhost:%{port}",
      "en_US" => "Malachi console serving at http://localhost:%{port}"
    },
    console_bundle_absent: %{
      "pt_BR" =>
        "Console sem bundle: %{dir} não tem index.html. O endpoint sobe e responde 503 até um release " <>
          "trazer o bundle",
      "en_US" =>
        "Console bundle absent: %{dir} holds no index.html. The endpoint starts and answers 503 until a " <>
          "release carries the bundle"
    },
    console_asset_unreadable: %{
      "pt_BR" => "Console deixou de fora %{path}: não foi possível ler (%{reason})",
      "en_US" => "Console left out %{path}: it could not be read (%{reason})"
    },
    console_listen_failed: %{
      "pt_BR" =>
        "Console não abriu a porta %{port} (%{reason}). O broker segue sem console; libere a porta ou " <>
          "defina MALACHI_CONSOLE_PORT e reinicie",
      "en_US" =>
        "Console could not listen on port %{port} (%{reason}). The broker carries on without a console; " <>
          "free the port or set MALACHI_CONSOLE_PORT and restart"
    },
    dashboard_cookie_plain: %{
      "pt_BR" =>
        "Cookie de sessão do dashboard sem Secure. O listener serve HTTP puro; defina " <>
          "MALACHI_DASHBOARD_SECURE_COOKIE=true se um proxy terminar TLS na frente dele",
      "en_US" =>
        "Dashboard session cookie is not marked Secure. This listener serves plain HTTP; set " <>
          "MALACHI_DASHBOARD_SECURE_COOKIE=true when a TLS-terminating proxy sits in front of it"
    },
    dashboard_cookie_secure: %{
      "pt_BR" =>
        "Cookie de sessão do dashboard marcado como Secure. Isso pressupõe um proxy terminando TLS na " <>
          "frente: alcançado direto por HTTP, o navegador descarta o cookie e o login falha sem erro",
      "en_US" =>
        "Dashboard session cookie is marked Secure. That assumes a TLS-terminating proxy in front: " <>
          "reached directly over HTTP, the browser drops the cookie and login fails with no error"
    },
    dashboard_cookie_secure_over_plain: %{
      "pt_BR" =>
        "Cookie Secure emitido, mas a requisição chegou com X-Forwarded-Proto: %{proto}. Se o navegador " <>
          "alcança o dashboard por HTTP puro, ele descarta o cookie e o login falha sem erro",
      "en_US" =>
        "Issued a Secure cookie, but the request arrived with X-Forwarded-Proto: %{proto}. If the browser " <>
          "reaches the dashboard over plain HTTP it drops the cookie and login fails with no error"
    },
    dashboard_cookie_plain_over_https: %{
      "pt_BR" =>
        "Requisição chegou com X-Forwarded-Proto: https, mas o cookie de sessão não está marcado como " <>
          "Secure. Defina MALACHI_DASHBOARD_SECURE_COOKIE=true para o navegador não o enviar por HTTP",
      "en_US" =>
        "Request arrived with X-Forwarded-Proto: https, but the session cookie is not marked Secure. Set " <>
          "MALACHI_DASHBOARD_SECURE_COOKIE=true so the browser will not send it over plain HTTP"
    },
    rate_limiter_started: %{
      "pt_BR" => "✅ RateLimiter iniciado",
      "en_US" => "✅ RateLimiter started"
    },
    rate_limiter_cleanup: %{
      "pt_BR" => "RateLimiter: %{count} buckets expirados limpos",
      "en_US" => "RateLimiter: cleaned %{count} expired buckets"
    },
    connection_limiter_started: %{
      "pt_BR" => "✅ ConnectionLimiter iniciado",
      "en_US" => "✅ ConnectionLimiter started"
    },
    auth_started: %{
      "pt_BR" => "✅ Sistema de autenticação iniciado",
      "en_US" => "✅ Authentication system started"
    },
    auth_success: %{
      "pt_BR" => "🔓 Usuário '%{username}' autenticado",
      "en_US" => "🔓 User '%{username}' authenticated"
    },
    auth_failed: %{
      "pt_BR" => "🔒 Falha na autenticação: '%{username}'",
      "en_US" => "🔒 Authentication failed: '%{username}'"
    },
    auth_user_not_found: %{
      "pt_BR" => "🔒 Usuário não encontrado: '%{username}'",
      "en_US" => "🔒 User not found: '%{username}'"
    },
    user_created: %{
      "pt_BR" => "👤 Usuário criado: '%{username}'",
      "en_US" => "👤 User created: '%{username}'"
    },
    user_removed: %{
      "pt_BR" => "👤 Usuário removido: '%{username}'",
      "en_US" => "👤 User removed: '%{username}'"
    },
    password_changed: %{
      "pt_BR" => "🔑 Senha alterada: '%{username}'",
      "en_US" => "🔑 Password changed: '%{username}'"
    },
    default_users_loaded: %{
      "pt_BR" => "👥 %{count} usuários padrão carregados",
      "en_US" => "👥 %{count} default users loaded"
    },
    user_role_changed: %{
      "pt_BR" => "Papel de console do usuário '%{username}' alterado para %{role}",
      "en_US" => "Console role of user '%{username}' set to %{role}"
    },
    default_user_role_pending: %{
      "pt_BR" => "Usuário padrão '%{username}' não criado: ele tem papel de console e %{reason}",
      "en_US" => "Default user '%{username}' not created: it carries a console role and %{reason}"
    },
    # Security hardening translations
    account_locked: %{
      "pt_BR" => "🔒 Conta bloqueada: '%{username}' (desbloqueio em %{time_remaining_ms}ms)",
      "en_US" => "🔒 Account locked: '%{username}' (unlock in %{time_remaining_ms}ms)"
    },
    session_hijack_attempt: %{
      "pt_BR" => "⚠️ Tentativa de sequestro de sessão detectada: '%{username}' (IP: %{ip})",
      "en_US" => "⚠️ Session hijack attempt detected: '%{username}' (IP: %{ip})"
    },
    audit_log_started: %{
      "pt_BR" => "✅ Sistema de auditoria iniciado (retenção: %{retention_days} dias)",
      "en_US" => "✅ Audit log system started (retention: %{retention_days} days)"
    },
    policy_defined: %{
      "pt_BR" => "📜 Política de armazenamento %{policy} definida por %{actor}",
      "en_US" => "📜 Storage policy %{policy} defined by %{actor}"
    },
    policy_deleted: %{
      "pt_BR" => "🗑️ Política de armazenamento %{policy} removida por %{actor}",
      "en_US" => "🗑️ Storage policy %{policy} deleted by %{actor}"
    },
    topic_policy_bound: %{
      "pt_BR" => "🔗 Tópico %{topic} vinculado à política %{policy} por %{actor}",
      "en_US" => "🔗 Topic %{topic} bound to policy %{policy} by %{actor}"
    },
    topic_policy_unbound: %{
      "pt_BR" => "🔗 Tópico %{topic} desvinculado da sua política por %{actor}",
      "en_US" => "🔗 Topic %{topic} detached from its policy by %{actor}"
    },
    audit_event_logged: %{
      "pt_BR" => "📝 Evento de auditoria registrado: %{event_type}",
      "en_US" => "📝 Audit event logged: %{event_type}"
    },
    lockout_manager_started: %{
      "pt_BR" => "✅ Gerenciador de bloqueio iniciado",
      "en_US" => "✅ Lockout manager started"
    },
    closing_connections: %{
      "pt_BR" => "🔌 Fechando %{count} conexões ativas...",
      "en_US" => "🔌 Closing %{count} active connections..."
    },
    graceful_shutdown: %{
      "pt_BR" => "⏳ Iniciando shutdown gracioso...",
      "en_US" => "⏳ Starting graceful shutdown..."
    },
    fence_failed: %{
      "pt_BR" => "⚠️ Cerca do segmento %{segment_id} falhou: %{reason}; o segmento pai segue aberto",
      "en_US" => "⚠️ Fence for segment %{segment_id} failed: %{reason}; the parent stays open"
    },
    roll_fence_failed: %{
      "pt_BR" =>
        "cerca do roll do segmento %{segment_id} falhou: %{reason}; o segmento segue aberto para escrita " <>
          "e a cerca é reenviada",
      "en_US" =>
        "roll fence for segment %{segment_id} failed: %{reason}; the segment stays open for writes and the " <>
          "fence is sent again"
    },
    seal_record_failed: %{
      "pt_BR" =>
        "❌ Segmento %{segment_id} foi cercado mas o selo no control plane falhou: %{reason}; o range " <>
          "não aceita escrita até um passe de heal reconciliar",
      "en_US" =>
        "❌ Segment %{segment_id} was fenced but recording its seal failed: %{reason}; the range takes " <>
          "no write until a heal pass reconciles it"
    },
    group_flush_failed: %{
      "pt_BR" => "⚠️ Flush do group commit falhou no pipeline %{pipeline}: %{reason}",
      "en_US" => "⚠️ Group commit flush failed on pipeline %{pipeline}: %{reason}"
    },
    group_commit_needs_rf1: %{
      "pt_BR" =>
        "⚠️ MALACHI_GROUP_COMMIT=true não se aplica com MALACHI_LOG_REPLICATION_FACTOR=%{rf}: o group commit " <>
          "do broker grava só o primário, então só vale com fator 1. Para um nó único defina " <>
          "MALACHI_LOG_REPLICATION_FACTOR=1, ou use MALACHI_REPLICATION_GROUP_COMMIT=true, que agrupa o fsync " <>
          "em todas as réplicas",
      "en_US" =>
        "⚠️ MALACHI_GROUP_COMMIT=true does not apply with MALACHI_LOG_REPLICATION_FACTOR=%{rf}: broker group " <>
          "commit writes the primary alone, so it needs a factor of 1. On a single node set " <>
          "MALACHI_LOG_REPLICATION_FACTOR=1, or use MALACHI_REPLICATION_GROUP_COMMIT=true, which batches the " <>
          "fsync on every replica"
    },
    data_shards_in_memory: %{
      "pt_BR" =>
        "⚠️ MALACHI_DATA_SHARDS=%{shards} é o modo de medição: a metadata de cada shard fica só em memória " <>
          "e nada escrito sobrevive a um restart; não há orphan sweep neste modo",
      "en_US" =>
        "⚠️ MALACHI_DATA_SHARDS=%{shards} is the measurement mode: each shard's metadata lives in memory only " <>
          "and nothing written survives a restart; there is no orphan sweep in this mode"
    },
    data_shards_ignored_clustered: %{
      "pt_BR" => "⚠️ MALACHI_DATA_SHARDS é ignorado quando o control plane é clusterizado; usando 1 shard",
      "en_US" => "⚠️ MALACHI_DATA_SHARDS is ignored when the control plane is clustered; using 1 shard"
    },
    # The envelope every boot gate refuses through (Malachi.StartupRefusal): one line with a fixed,
    # searchable prefix, wrapping a detail that is a complete sentence of its own, one key per
    # operator action.
    startup_refused: %{
      "pt_BR" => "RECUSANDO INICIAR (exit 78): %{detail}",
      "en_US" => "REFUSING TO START (exit 78): %{detail}"
    },
    # Data-directory format marker (Malachi.Storage.FormatMarker).
    data_format_too_new: %{
      "pt_BR" =>
        "o marker de formato %{path} registra o formato %{format}, gravado pelo release %{written_by}, e este " <>
          "binário entende no máximo o formato %{supported}. Inicie o release %{requires} ou mais novo; " <>
          "não apague o marker, isso deixaria este binário sobrescrever dados que ele não sabe ler",
      "en_US" =>
        "the format marker %{path} records format %{format}, written by release %{written_by}, and this " <>
          "binary understands at most format %{supported}. Start release %{requires} or newer; do not " <>
          "delete the marker, that would let this binary overwrite data it cannot read"
    },
    # Log directory against the control plane (Malachi.Storage.DataDirGuard).
    data_dir_cluster_renamed: %{
      "pt_BR" =>
        "%{path} pertence ao control plane %{recorded}, e este nó (%{node}) iniciaria nele como %{cluster}. " <>
          "O anel e os membros do ra são um só por diretório ra e nome de nó, seja qual for o nome do cluster, " <>
          "então os dois control planes se misturariam e o orphan sweep poderia apagar os segmentos do " <>
          "primeiro. Volte MALACHI_LOG_CLUSTER para %{recorded}, ou forme %{cluster} com outro " <>
          "MALACHI_LOG_DATA_DIR e outro MALACHI_RA_DATA_DIR e mova os dados com um cliente",
      "en_US" =>
        "%{path} belongs to the control plane %{recorded}, and this node (%{node}) would start on it as " <>
          "%{cluster}. The ring store and the ra members are one per ra directory and node name, whatever the " <>
          "cluster is called, so the two control planes would mix and the orphan sweep could delete the " <>
          "first one's segments. Set MALACHI_LOG_CLUSTER back to %{recorded}, or form %{cluster} with another " <>
          "MALACHI_LOG_DATA_DIR and another MALACHI_RA_DATA_DIR and move the data with a client"
    },
    data_dir_cluster_marker_unreadable: %{
      "pt_BR" => "um nome ilegível (o arquivo malachi.cluster não tem exatamente uma linha cluster=<nome>)",
      "en_US" => "an unreadable name (the malachi.cluster file does not hold exactly one cluster=<name> line)"
    },
    data_dir_unknown_segments: %{
      "pt_BR" =>
        "%{path} tem %{count} diretórios de segmento (%{names}) e o control plane %{cluster} seria formado " <>
          "agora neste nó, com o nome de nó %{node}, sem histórico deles, então o orphan sweep poderia apagá-los. " <>
          "Causas: o nome do nó mudou (fixe --hostname ou RELEASE_NODE), o diretório MALACHI_RA_DATA_DIR se " <>
          "perdeu, MALACHI_LOG_CLUSTER mudou, ou os dados vêm de um release que guardava a metadata de um nó " <>
          "único em memória. Volte o nome do nó, o diretório ra ou o nome do cluster; ou defina " <>
          "MALACHI_ADOPT_ORPHANED_LOG_DIR=true para iniciar mesmo assim. Num nó único o sweep então os " <>
          "remove; num membro de cluster cujos pares ainda guardam o control plane, adotar é o caminho de " <>
          "volta normal e o sweep remove só o que nenhum dono lista",
      "en_US" =>
        "%{path} holds %{count} segment directories (%{names}) and the control plane %{cluster} would be " <>
          "formed now on this node, under the node name %{node}, with no history of them, so the orphan " <>
          "sweep could delete them. Causes: the node name changed (pin --hostname or RELEASE_NODE), the " <>
          "MALACHI_RA_DATA_DIR directory was lost, MALACHI_LOG_CLUSTER changed, or the data comes from a " <>
          "release that kept a single node's metadata in memory. Bring back the node name, the ra directory " <>
          "or the cluster name; or set MALACHI_ADOPT_ORPHANED_LOG_DIR=true to start anyway. On a single node " <>
          "the sweep then removes them; on a cluster member whose peers still hold the control plane, " <>
          "adopting is the normal way back and the sweep removes only what no owner lists"
    },
    data_dir_grow_unsupported: %{
      "pt_BR" =>
        "o control plane %{cluster} foi iniciado neste nó (%{node}) como cluster de um membro, e a " <>
          "configuração agora lista também %{others}. Crescer um nó único em cluster no lugar não é " <>
          "suportado: este nó voltaria sozinho enquanto os outros formariam outro cluster com o mesmo nome. " <>
          "Remova os outros nós de MALACHI_LOG_NODES, ou forme o cluster novo com outro MALACHI_LOG_CLUSTER " <>
          "e outro MALACHI_LOG_DATA_DIR e mova os dados com um cliente",
      "en_US" =>
        "the control plane %{cluster} was started on this node (%{node}) as a one-member cluster, and the " <>
          "configuration now also lists %{others}. Growing a single node into a cluster in place is not " <>
          "supported: this node would come back alone while the others formed another cluster under the " <>
          "same name. Remove the other nodes from MALACHI_LOG_NODES, or form the new cluster under another " <>
          "MALACHI_LOG_CLUSTER with another MALACHI_LOG_DATA_DIR and move the data with a client"
    },
    data_dir_reshard_unsupported: %{
      "pt_BR" =>
        "o control plane %{cluster} já rodou sem sharding neste nó (%{node}), e MALACHI_LOG_VNODES pede " <>
          "vnodes. Converter um control plane para sharding no lugar não é suportado: o anel sharded valeria " <>
          "dali em diante e a metadata atual dos tópicos não seria mais lida, então o orphan sweep apagaria " <>
          "os segmentos deles. Nada foi gravado: remova MALACHI_LOG_VNODES para iniciar como antes. Para fazer " <>
          "sharding, forme um cluster novo com outro MALACHI_LOG_CLUSTER, outro MALACHI_LOG_DATA_DIR e outro " <>
          "MALACHI_RA_DATA_DIR (o anel é um só por diretório ra) e mova os dados com um cliente",
      "en_US" =>
        "the control plane %{cluster} already ran unsharded on this node (%{node}), and MALACHI_LOG_VNODES asks " <>
          "for vnodes. Converting a control plane to sharding in place is not supported: the sharded ring would " <>
          "rule from then on and the topics' current metadata would no longer be read, so the orphan sweep " <>
          "would delete their segments. Nothing was written: remove MALACHI_LOG_VNODES to start as before. To " <>
          "shard, form a new cluster under another MALACHI_LOG_CLUSTER with another MALACHI_LOG_DATA_DIR and " <>
          "another MALACHI_RA_DATA_DIR (there is one ring store per ra directory) and move the data with a client"
    },
    data_dir_resharded: %{
      "pt_BR" =>
        "o control plane %{cluster} rodou sem sharding neste nó (%{node}), e um anel sharded já está gravado " <>
          "para ele (por outro nó, por uma release anterior que convertia control planes no lugar, ou por " <>
          "uma release anterior neste nó sob outro MALACHI_LOG_CLUSTER no mesmo diretório ra). A " <>
          "metadata sem sharding dos tópicos não é mais lida e o anel vale mais que MALACHI_LOG_VNODES, então " <>
          "não há volta no lugar; iniciar deixaria o orphan sweep apagar os segmentos que essa metadata " <>
          "descreve. Forme o cluster de novo com outro MALACHI_LOG_CLUSTER, outro MALACHI_LOG_DATA_DIR e outro " <>
          "MALACHI_RA_DATA_DIR, e guarde os diretórios deste nó até os dados não serem mais necessários",
      "en_US" =>
        "the control plane %{cluster} ran unsharded on this node (%{node}), and a sharded ring is already " <>
          "recorded for it (by another node, by an earlier release that converted control planes in place, or by " <>
          "an earlier release on this node under another MALACHI_LOG_CLUSTER over the same ra directory). " <>
          "The topics' unsharded metadata is no longer read and the ring outranks MALACHI_LOG_VNODES, so there " <>
          "is no way back in place; starting would let the orphan sweep delete the segments that metadata " <>
          "describes. Form the cluster again under another MALACHI_LOG_CLUSTER with another " <>
          "MALACHI_LOG_DATA_DIR and another MALACHI_RA_DATA_DIR, and keep this node's directories until the " <>
          "data is no longer wanted"
    },
    data_dir_membership_unknown: %{
      "pt_BR" =>
        "o control plane %{cluster} foi iniciado neste nó (%{node}) e a configuração lista outros nós, mas o " <>
          "membro do ring store não informou a sua membership dentro de MALACHI_LOG_RING_BOOT_TIMEOUT_MS. Sem ela não dá " <>
          "para saber se é um cluster de um membro que cresceria no lugar, o que não é suportado. Veja o log " <>
          "do ra deste nó, ou aumente o timeout se o replay do log for longo",
      "en_US" =>
        "the control plane %{cluster} was started on this node (%{node}) and the configuration lists other " <>
          "nodes, but the ring store member did not report its membership within MALACHI_LOG_RING_BOOT_TIMEOUT_MS. " <>
          "Without it there is no telling whether this is a one-member cluster about to grow in place, which " <>
          "is not supported. Check this node's ra log, or raise the timeout if the log replay is long"
    },
    data_dir_adopted: %{
      "pt_BR" =>
        "⚠️ MALACHI_ADOPT_ORPHANED_LOG_DIR=true: iniciando com %{count} diretórios de segmento em %{path} " <>
          "que este control plane, formado agora, não conhece (%{names}); o orphan sweep remove os que " <>
          "nenhum dono listar",
      "en_US" =>
        "⚠️ MALACHI_ADOPT_ORPHANED_LOG_DIR=true: starting with %{count} segment directories in %{path} " <>
          "that this control plane, formed now, does not know (%{names}); the orphan sweep removes the ones " <>
          "no owner lists"
    },
    data_format_marker_invalid: %{
      "pt_BR" =>
        "o marker de formato %{path} não é válido (%{reason}). Restaure-o de um backup ou de outro nó; " <>
          "o nó não inicia sem saber qual formato o diretório contém",
      "en_US" =>
        "the format marker %{path} is not valid (%{reason}). Restore it from a backup or another node; " <>
          "the node does not start without knowing which format the directory holds"
    },
    data_format_marker_io_failed: %{
      "pt_BR" =>
        "não foi possível ler ou gravar o marker de formato %{path} (%{reason}). Corrija o volume ou as " <>
          "permissões e inicie de novo",
      "en_US" =>
        "the format marker %{path} could not be read or written (%{reason}). Fix the volume or its " <>
          "permissions and start again"
    },
    # Cluster feature flags (Malachi.Cluster.ClusterFlags). A flag only ever goes from off to on, and
    # only once every node advertises the capability it names.
    cluster_flag_enabled: %{
      "pt_BR" => "flag de cluster %{flag} ligado por um operador",
      "en_US" => "cluster flag %{flag} enabled by an operator"
    },
    cluster_flag_enable_refused: %{
      "pt_BR" => "recusando ligar o flag de cluster %{flag}: %{nodes} não anunciam essa capability",
      "en_US" => "refusing to enable the cluster flag %{flag}: %{nodes} do not advertise that capability"
    },
    cluster_flag_adopted: %{
      "pt_BR" => "flag de cluster %{flag} adotado neste nó",
      "en_US" => "cluster flag %{flag} adopted on this node"
    },
    member_incarnation_unusable: %{
      "pt_BR" =>
        "não foi possível reservar a incarnation deste nó em %{path} (%{reason}). O nó não inicia: " <>
          "subir sem ela significaria anunciar um número abaixo do que os pares lembram, e nada corrige " <>
          "isso depois, porque um nó vivo nunca é suspeitado e portanto nunca refuta. Conserte o volume, " <>
          "ou restaure o arquivo de outro backup deste mesmo nó",
      "en_US" =>
        "could not reserve this node's incarnation at %{path} (%{reason}). The node does not start: " <>
          "coming up without it would mean announcing a number below what peers remember, and nothing " <>
          "corrects that afterwards, because a live node is never suspected and so never refutes. Fix " <>
          "the volume, or restore the file from a backup of this same node"
    },
    member_incarnation_ceiling_lost: %{
      "pt_BR" =>
        "não foi possível gravar um teto de incarnation novo em %{incarnation} (%{reason}); parando o " <>
          "nó para que ele reserve outro ao subir. Seguir serviria agora e deixaria um restart futuro " <>
          "voltar abaixo do que os pares lembram, onde nada mais corrige",
      "en_US" =>
        "could not record a new incarnation ceiling at %{incarnation} (%{reason}); stopping the node so " <>
          "it reserves one it can trust on the way back up. Carrying on would serve now and let a " <>
          "future restart resume below what peers remember, where nothing corrects it"
    },
    advertised_host_missing: %{
      "pt_BR" =>
        "este nó tem peers (MALACHI_LOG_NODES) mas MALACHI_ADVERTISED_HOST não está definido: os clientes não teriam um endereço para alcançá-lo",
      "en_US" =>
        "this node has peers (MALACHI_LOG_NODES) but MALACHI_ADVERTISED_HOST is not set: clients would have no address to reach it at"
    },
    advertised_host_loopback: %{
      "pt_BR" =>
        "este nó tem peers (MALACHI_LOG_NODES) mas MALACHI_ADVERTISED_HOST=%{host} é um endereço de loopback ou não especificado: um cliente em outra máquina discaria para si mesmo",
      "en_US" =>
        "this node has peers (MALACHI_LOG_NODES) but MALACHI_ADVERTISED_HOST=%{host} is a loopback or unspecified address: a client on another machine would dial itself"
    },
    cluster_flag_missing_capability: %{
      "pt_BR" =>
        "o cluster ligou %{flags}, que este binário não suporta; ele anuncia %{capabilities}. " <>
          "Inicie um release que suporte %{flags}. Um flag nunca volta a ser desligado, então este nó " <>
          "não pode servir até lá",
      "en_US" =>
        "the cluster has enabled %{flags}, which this binary does not support; it advertises " <>
          "%{capabilities}. Start a release that supports %{flags}. A flag is never turned back off, " <>
          "so this node cannot serve until then"
    },
    data_format_marker_created_fresh: %{
      "pt_BR" => "Marker de formato criado em %{path} (formato %{format}, diretório novo)",
      "en_US" => "Format marker created at %{path} (format %{format}, fresh directory)"
    },
    data_format_marker_created_existing: %{
      "pt_BR" =>
        "Marker de formato criado em %{path} (formato %{format}) para um diretório gravado antes do marker existir",
      "en_US" => "Format marker created at %{path} (format %{format}) for a directory written before the marker existed"
    },
    ring_env_ignored: %{
      "pt_BR" =>
        "⚠️ O anel durável (versão %{version}, %{durable} vnodes) vence MALACHI_LOG_VNODES=%{env}; " <>
          "a env foi ignorada. Veja `mix malachi.ring --show`",
      "en_US" =>
        "⚠️ The durable ring (version %{version}, %{durable} vnodes) wins over MALACHI_LOG_VNODES=%{env}; " <>
          "the environment was ignored. See `mix malachi.ring --show`"
    },
    ring_seeded: %{
      "pt_BR" => "Anel semeado a partir de MALACHI_LOG_VNODES=%{env}: nenhum anel durável existia",
      "en_US" => "Seeded the ring from MALACHI_LOG_VNODES=%{env}: no durable ring existed"
    },
    ring_seed_race_lost: %{
      "pt_BR" => "Outro nó semeou o anel primeiro (versão %{version}, %{durable} vnodes); adotando o dele",
      "en_US" => "Another node seeded the ring first (version %{version}, %{durable} vnodes); adopting it"
    },
    memory_gc_complete: %{
      "pt_BR" => "GC do sistema concluído: %{reclaimed} MB recuperados (%{before} MB -> %{after} MB)",
      "en_US" => "System GC complete: reclaimed %{reclaimed} MB (%{before} MB -> %{after} MB)"
    },
    memory_high_usage: %{
      "pt_BR" =>
        "Uso de memória alto: %{total} MB no total (processos: %{processes} MB, ETS: %{ets} MB, " <>
          "binários: %{binary} MB)",
      "en_US" =>
        "High memory usage: %{total} MB total (processes: %{processes} MB, ETS: %{ets} MB, " <>
          "binary: %{binary} MB)"
    },
    memory_top_consumers: %{
      "pt_BR" => "Maiores consumidores de memória: %{consumers}",
      "en_US" => "Top memory consumers: %{consumers}"
    },
    atom_usage_critical: %{
      "pt_BR" =>
        "CRÍTICO: tabela de atoms em %{usage}% (%{count}/%{limit}). Possível ataque de exaustão de " <>
          "atoms ou vazamento de atoms dinâmicos.",
      "en_US" =>
        "CRITICAL: Atom table usage at %{usage}% (%{count}/%{limit}). Possible atom exhaustion attack " <>
          "or dynamic atom leak."
    },
    atom_usage_warning: %{
      "pt_BR" => "ATENÇÃO: tabela de atoms em %{usage}% (%{count}/%{limit}). Monitore possíveis vazamentos de atoms.",
      "en_US" => "WARNING: Atom table usage at %{usage}% (%{count}/%{limit}). Monitor for potential atom leaks."
    },
    # Deliberately identical in both locales: this line is parsed by log shippers, and translating the
    # prefix or the payload would break them. It goes through I18n so the convention holds without
    # exception, not because the text varies.
    audit_log_line: %{
      "pt_BR" => "[AUDIT] %{json}",
      "en_US" => "[AUDIT] %{json}"
    },
    # Malachi.UnexpectedMessage: a long-lived server received something it has no clause for. `server` is
    # the server's label and `message` only the term's shape (strings are elided before this is called).
    unexpected_cast: %{
      "pt_BR" => "processo %{server} descartando cast inesperado: %{message}",
      "en_US" => "%{server} process dropping an unexpected cast: %{message}"
    },
    unexpected_info: %{
      "pt_BR" => "processo %{server} ignorando mensagem inesperada: %{message}",
      "en_US" => "%{server} process ignoring an unexpected message: %{message}"
    },
    unexpected_call: %{
      "pt_BR" => "processo %{server} respondendo {:error, :unknown_call} a uma chamada inesperada: %{message}",
      "en_US" => "%{server} process answering {:error, :unknown_call} to an unexpected call: %{message}"
    },
    # An operator-supplied setting the process that uses it could not accept (`Malachi.Config.checked/4`)
    setting_invalid: %{
      "pt_BR" => "⚠️ %{setting} não aceita o valor %{value}; usando o padrão %{default}",
      "en_US" => "⚠️ %{setting} does not accept the value %{value}; using the default %{default}"
    },
    unexpected_messages_log_limit: %{
      "pt_BR" =>
        "processo %{server} já registrou %{limit} formatos de mensagem inesperada; os próximos são só " <>
          "contados em malachi_unexpected_messages_total",
      "en_US" =>
        "%{server} process has logged %{limit} unexpected message shapes; further ones are only counted " <>
          "in malachi_unexpected_messages_total"
    },
    scrub_metadata_unavailable: %{
      "pt_BR" => "⚠️ scrub pulou a passada: o plano de controle não respondeu (%{reason}); a próxima tenta de novo",
      "en_US" => "⚠️ the scrub skipped the pass: the control plane did not answer (%{reason}); the next one tries again"
    },
    scrub_segment_damaged: %{
      "pt_BR" => "scrub encontrou %{segment_id} danificado (%{reason} no byte %{position})%{outcome}",
      "en_US" => "scrub found %{segment_id} damaged (%{reason} at byte %{position})%{outcome}"
    },
    scrub_repair_succeeded: %{
      "pt_BR" => ": reparado a partir de uma réplica íntegra",
      "en_US" => ": repaired from an intact replica"
    },
    scrub_repair_failed_refetch: %{
      "pt_BR" =>
        ": reparo FALHOU no meio do refetch (%{reason}), esta cópia fica incompleta até uma passagem posterior concluir",
      "en_US" => ": repair FAILED mid-refetch (%{reason}), this copy is incomplete until a later pass finishes it"
    },
    scrub_repair_not_done: %{
      "pt_BR" => ": NÃO reparado (%{reason}), esta cópia segue danificada e seus bytes continuam em disco",
      "en_US" => ": NOT repaired (%{reason}), this copy stays damaged and its bytes are still on disk"
    },
    replication_catchup_failed: %{
      "pt_BR" => "catch-up de %{segment_id} (%{from}..%{to}) falhou: %{reason}",
      "en_US" => "catch-up for %{segment_id} (%{from}..%{to}) failed: %{reason}"
    },
    replication_sealed_segment_damaged: %{
      "pt_BR" =>
        "segmento %{segment_id} falhou na verificação no byte %{position} (%{reason}, %{bytes} bytes " <>
          "ilegíveis): segmento selado, esta cópia precisa de reparo a partir de uma réplica íntegra",
      "en_US" =>
        "segment %{segment_id} failed verification at byte %{position} (%{reason}, %{bytes} bytes " <>
          "unreadable): sealed segment, this copy needs repair from an intact replica"
    },
    replication_active_segment_damaged: %{
      "pt_BR" =>
        "segmento %{segment_id} falhou na verificação no byte %{position} (%{reason}, %{bytes} bytes " <>
          "ilegíveis): segmento ativo, dano depois de um frame completo",
      "en_US" =>
        "segment %{segment_id} failed verification at byte %{position} (%{reason}, %{bytes} bytes " <>
          "unreadable): active segment, damage past a complete frame"
    },
    replication_partial_write_dropped: %{
      "pt_BR" => "segmento %{segment_id} descartou %{bytes} bytes de uma escrita parcial",
      "en_US" => "segment %{segment_id} dropped %{bytes} bytes of a partial write"
    },
    replication_segment_storage_failed: %{
      "pt_BR" =>
        "a cópia do segmento %{segment_id} neste nó falhou no armazenamento (%{reason}): ela não será " <>
          "reescrita, as requisições para ela são recusadas até um restart ou delete, e a passagem de cura " <>
          "sela o segmento nas réplicas restantes e repõe esta cópia em outro broker",
      "en_US" =>
        "segment %{segment_id}'s copy on this node failed in storage (%{reason}): it will not be written " <>
          "again, requests for it are refused until a restart or delete, and the healing pass seals the " <>
          "segment on the remaining replicas and replaces this copy on another broker"
    },
    heal_repair_failed: %{
      "pt_BR" => "passagem de cura não conseguiu reparar %{count}: %{failures}",
      "en_US" => "healing pass could not repair %{count}: %{failures}"
    },
    heal_failed_copy_replaced: %{
      "pt_BR" =>
        "%{count} cópia(s) que falharam no armazenamento foram repostas em outro broker e apagadas de onde " <>
          "falharam: %{copies}",
      "en_US" =>
        "replaced %{count} copy(ies) that failed in storage on another broker and deleted them where they " <>
          "failed: %{copies}"
    },
    heal_orphaned_fence_reconciled: %{
      "pt_BR" =>
        "%{count} segmento(s) reconciliado(s) cujo store estava cercado enquanto o control plane ainda " <>
          "os chamava de ativos: %{segments}. Os ranges deles recusavam toda escrita até agora, então " <>
          "um seal de cerca que não persiste merece investigação a montante",
      "en_US" =>
        "reconciled %{count} segment(s) whose store was fenced while the control plane still called " <>
          "them active: %{segments}. Their ranges were refusing every write until now, so a fence's " <>
          "seal failing to land is worth investigating upstream"
    },
    heal_sealed_copy_trimmed: %{
      "pt_BR" =>
        "%{count} cópia(s) de segmento selado voltaram ao comprimento que o control plane registrou, " <>
          "descartando %{records} registro(s) além do fim selado: %{copies}. Nenhum deles foi " <>
          "reconhecido a um cliente nem servido em leitura, mas uma cópia à frente do selo veio de uma " <>
          "escrita que perdeu o quórum, então vale investigar o failover daquele segmento",
      "en_US" =>
        "brought %{count} sealed segment copy(ies) back to the length the control plane recorded, " <>
          "dropping %{records} record(s) past the sealed end: %{copies}. None was acknowledged to a " <>
          "client or served to a read, but a copy ahead of its seal came from a write that lost quorum, " <>
          "so that segment's failover is worth investigating"
    },
    heal_sealed_copy_not_settled: %{
      "pt_BR" =>
        "%{count} cópia(s) de segmento selado não puderam ser levadas ao comprimento que o control plane " <>
          "registrou e seguem divergentes: %{copies}. Uma passagem posterior tenta de novo, mas uma " <>
          "cópia que nunca assenta guarda bytes que o selo dela exclui",
      "en_US" =>
        "could not bring %{count} sealed segment copy(ies) to the length the control plane recorded, " <>
          "and they are still divergent: %{copies}. A later pass tries again, but a copy that never " <>
          "settles is holding bytes its own seal excludes"
    },
    heal_orphaned_fence_unrecorded: %{
      "pt_BR" =>
        "não foi possível registrar o seal de %{count} segmento(s) cercado(s): %{segments}. Os ranges " <>
          "deles não aceitam escrita até uma passagem posterior conseguir, então o control plane é o " <>
          "lugar a olhar",
      "en_US" =>
        "could not record the seal for %{count} fenced segment(s): %{segments}. Their ranges take no " <>
          "write until a later pass succeeds, so the control plane is the thing to look at"
    },
    heal_seal_no_quorum: %{
      "pt_BR" =>
        "segmento %{segment_id} não pode ser selado para failover: %{answered} de %{replicas} réplicas " <>
          "responderam, e são precisas %{needed} para cobrir toda escrita confirmada. Seu range fica " <>
          "bloqueado para escrita até elas voltarem, porque selar em menos poderia descartar escritas já " <>
          "confirmadas",
      "en_US" =>
        "segment %{segment_id} cannot be sealed for failover: %{answered} of %{replicas} replicas " <>
          "answered, and %{needed} are needed to cover every acknowledged write. Its range is blocked for " <>
          "writes until they return, because sealing on fewer could discard acknowledged writes"
    },
    auto_rebalance_committed: %{
      "pt_BR" => "rebalanceamento automático aplicado: %{applied}",
      "en_US" => "auto-rebalance committed: %{applied}"
    },
    auto_rebalance_partial: %{
      "pt_BR" => "rebalanceamento automático parcial: aplicado=%{applied} falha=%{failure}",
      "en_US" => "auto-rebalance partial: applied=%{applied} failure=%{failure}"
    },
    vnode_coordinators_down: %{
      "pt_BR" => "coordenadores do vnode %{vnode} caíram (%{reason}); reiniciando no próximo reconcile",
      "en_US" => "vnode %{vnode} coordinators went down (%{reason}); restarting on the next reconcile"
    },
    vnode_placement_unreadable: %{
      "pt_BR" =>
        "não foi possível ler a colocação de vnodes (%{reason}); mantendo os coordenadores que este nó já roda até conseguir ler de novo",
      "en_US" =>
        "the vnode placement could not be read (%{reason}); keeping the coordinators this node already runs until it can be read again"
    },
    heal_metadata_unavailable: %{
      "pt_BR" => "passada de heal pulada: não foi possível ler a metadata (%{reason})",
      "en_US" => "heal pass skipped: the metadata could not be read (%{reason})"
    },
    heal_commands_unapplied: %{
      "pt_BR" =>
        "passada de heal não conseguiu entregar seus comandos ao broker (%{reason}); os não entregues esperam a próxima passada, e um selo de failover entre eles pode não ser planejado de novo (#269)",
      "en_US" =>
        "heal pass could not hand its commands to the broker (%{reason}); the ones not handed over wait for the next pass, and a failover seal among them may not be planned again (#269)"
    },
    retention_metadata_unavailable: %{
      "pt_BR" => "varredura de retenção pulada: não foi possível ler a metadata (%{reason})",
      "en_US" => "retention sweep skipped: the metadata could not be read (%{reason})"
    },
    vnode_member_resumed: %{
      "pt_BR" => "membro deste nó no vnode %{vnode} retomado a partir do log persistido",
      "en_US" => "resumed this node's member of vnode %{vnode} from its persisted log"
    },
    vnode_member_resume_failed: %{
      "pt_BR" => "não foi possível retomar o membro deste nó no vnode %{vnode}: %{reason}",
      "en_US" => "could not resume this node's member of vnode %{vnode}: %{reason}"
    },
    vnode_members_resume_raised: %{
      "pt_BR" =>
        "não foi possível retomar os membros de vnode deste nó: %{reason}; a passada segue sem retomar, com os coordenadores conforme a liderança que conseguir ler",
      "en_US" =>
        "could not resume this node's vnode members: %{reason}; the pass goes on without resuming, with the coordinators the leadership it can read calls for"
    },
    vnode_placement_recovered: %{
      "pt_BR" => "a colocação de vnodes voltou a ser legível; reconciliando os coordenadores deste nó",
      "en_US" => "the vnode placement is readable again; reconciling this node's coordinators"
    },
    ra_machine_version_unsupported: %{
      "pt_BR" =>
        "membro ra %{server} (%{machine}) parou de aplicar entradas: a versão efetiva do cluster é %{effective} e este nó suporta %{supported}; atualize o binário ou suba o pin",
      "en_US" =>
        "ra member %{server} (%{machine}) stopped applying entries: the cluster's effective version is %{effective} and this node supports %{supported}; upgrade the binary or raise the pin"
    },
    ra_machine_version_recovered: %{
      "pt_BR" => "membro ra %{server} (%{machine}) voltou a suportar a versão efetiva %{effective}",
      "en_US" => "ra member %{server} (%{machine}) supports the effective version %{effective} again"
    },
    # Malachi.BrokerServer: the control plane reconcile runs off the broker's loop, so its two ways of
    # not finishing are the only place an operator hears about them. One key each: they are different
    # failures and lead to different checks.
    broker_reconcile_task_down: %{
      "pt_BR" =>
        "⚠️ O reconcile do control plane caiu (%{reason}); o nó segue servindo a visão que já tem, " <>
          "que envelhece até o próximo tick conseguir ler",
      "en_US" =>
        "⚠️ The control plane reconcile crashed (%{reason}); the node keeps serving the view it already " <>
          "holds, which goes stale until a later tick manages to read"
    },
    broker_reconcile_task_timeout: %{
      "pt_BR" =>
        "⚠️ O reconcile do control plane passou de %{timeout_ms}ms e foi encerrado; alguma chamada " <>
          "remota não retornou (o bootstrap de um vnode não tem timeout próprio) e o nó segue " <>
          "servindo a visão que já tem",
      "en_US" =>
        "⚠️ The control plane reconcile overran %{timeout_ms}ms and was killed; a remote call did not " <>
          "return (a vnode bootstrap has no timeout of its own) and the node keeps serving the view it " <>
          "already holds"
    },
    ring_publish_refused_completing: %{
      "pt_BR" =>
        "⚠️ O store do anel recusou a publicação ao concluir um split interrompido do vnode %{vnode} " <>
          "(%{reason}); o anel não avançou e uma retomada do lease vai tentar de novo",
      "en_US" =>
        "⚠️ The ring store refused the publication while completing an interrupted split for vnode " <>
          "%{vnode} (%{reason}); the ring was not advanced and a later lease takeover will retry"
    },
    ring_publish_refused_clearing: %{
      "pt_BR" =>
        "⚠️ O store do anel recusou a publicação ao limpar um split abortado do vnode %{vnode} " <>
          "(%{reason}); a intenção segue registrada e uma retomada do lease vai tentar de novo",
      "en_US" =>
        "⚠️ The ring store refused the publication while clearing an aborted split for vnode %{vnode} " <>
          "(%{reason}); the intent stays recorded and a later lease takeover will retry"
    },
    ring_unseeded: %{
      "pt_BR" =>
        "O anel inicial não pôde ser gravado (%{reason}). Recusando o boot em vez de servir um anel " <>
          "que o cluster nunca aceitou; verifique se um quórum do cluster do anel está de pé",
      "en_US" =>
        "The initial ring could not be recorded (%{reason}). Refusing to boot rather than serving a " <>
          "ring the cluster never accepted; check that a quorum of the ring cluster is up"
    },
    ring_unreadable: %{
      "pt_BR" =>
        "O anel durável não pôde ser lido em %{timeout}ms (%{reason}). Recusando o boot em vez de " <>
          "servir um anel possivelmente errado; ajuste MALACHI_LOG_RING_BOOT_TIMEOUT_MS ou suba os demais nós",
      "en_US" =>
        "The durable ring could not be read within %{timeout}ms (%{reason}). Refusing to boot rather " <>
          "than serving a possibly wrong ring; raise MALACHI_LOG_RING_BOOT_TIMEOUT_MS or start the other nodes"
    },
    # Audit log translations
    audit_log_file_enabled: %{
      "pt_BR" => "Saída de auditoria em arquivo habilitada: %{path} (max: %{max_mb}MB)",
      "en_US" => "Audit log file output enabled: %{path} (max: %{max_mb}MB)"
    },
    audit_log_file_failed: %{
      "pt_BR" => "Falha ao abrir arquivo de auditoria %{path}: %{reason}",
      "en_US" => "Failed to open audit log file %{path}: %{reason}"
    },
    audit_log_stdout_enabled: %{
      "pt_BR" => "Saída de auditoria em stdout habilitada",
      "en_US" => "Audit log stdout output enabled"
    },
    audit_log_cleanup: %{
      "pt_BR" => "Limpeza de auditoria: %{count} eventos antigos removidos",
      "en_US" => "Audit log cleanup: removed %{count} old events"
    },
    audit_log_file_reopen_failed: %{
      "pt_BR" => "Falha ao reabrir arquivo de auditoria após rotação: %{reason}",
      "en_US" => "Failed to reopen audit log file after rotation: %{reason}"
    },
    # Lockout manager translations
    account_unlocked_all_ips: %{
      "pt_BR" => "🔓 Conta desbloqueada para todos os IPs: '%{username}'",
      "en_US" => "🔓 Account unlocked for all IPs: '%{username}'"
    },
    account_unlocked: %{
      "pt_BR" => "🔓 Conta desbloqueada: '%{username}' (IP: %{ip})",
      "en_US" => "🔓 Account unlocked: '%{username}' (IP: %{ip})"
    },
    lockout_store_unavailable: %{
      "pt_BR" => "Store de bloqueios indisponível em %{operation}: %{reason}",
      "en_US" => "Lockout store unavailable on %{operation}: %{reason}"
    },
    # Session manager translations
    invalid_cidr_range: %{
      "pt_BR" => "Faixa CIDR inválida em trusted_proxy_ranges: %{range}",
      "en_US" => "Invalid CIDR range in trusted_proxy_ranges: %{range}"
    },
    # Config validator translations
    warning_no_admin: %{
      "pt_BR" =>
        "╔════════════════════════════════════════════════════════════╗\n║ AVISO: Nenhum usuário admin configurado                      ║\n╠════════════════════════════════════════════════════════════╣\n║ Não será possível gerenciar usuários, desbloquear contas,   ║\n║ ou realizar ações administrativas.                           ║\n║                                                              ║\n║ Configure um usuário admin:                                  ║\n║   MALACHI_ADMIN_PASS=\"<senha_forte>\"                      ║\n╚════════════════════════════════════════════════════════════╝",
      "en_US" =>
        "╔════════════════════════════════════════════════════════════╗\n║ WARNING: No admin user configured                          ║\n╠════════════════════════════════════════════════════════════╣\n║ You will not be able to manage users, unlock accounts,    ║\n║ or perform administrative actions.                         ║\n║                                                            ║\n║ Configure an admin user:                                   ║\n║   MALACHI_ADMIN_PASS=\"<strong_password>\"                 ║\n╚════════════════════════════════════════════════════════════╝"
    },
    warning_weak_passwords: %{
      "pt_BR" =>
        "⚠️  Modo desenvolvimento: Senhas fracas detectadas para usuários: %{usernames}\nAceitável em dev/test mas NÃO em produção.",
      "en_US" =>
        "⚠️  Development mode: Weak passwords detected for users: %{usernames}\nThis is acceptable in dev/test but NOT in production."
    },
    warning_short_passwords: %{
      "pt_BR" =>
        "⚠️  Modo desenvolvimento: Senhas curtas detectadas para usuários: %{usernames}\nTamanho mínimo: %{min_length} caracteres",
      "en_US" =>
        "⚠️  Development mode: Short passwords detected for users: %{usernames}\nMinimum length: %{min_length} characters"
    },
    warning_no_admin_dev: %{
      "pt_BR" => "⚠️  Modo desenvolvimento: Nenhum usuário admin configurado",
      "en_US" => "⚠️  Development mode: No admin user configured"
    },
    warning_generated_admin_ephemeral: %{
      "pt_BR" =>
        "⚠️  Admin gerado sobre um store efêmero: MALACHI_RA_DATA_DIR não está setado, então o log do " <>
          "ra fica em um diretório temporário que não sobrevive a um container recriado ou a um reboot. " <>
          "Quando ele se perde, o admin gerado vai junto e uma senha nova é gerada e logada. Aponte " <>
          "MALACHI_RA_DATA_DIR para um volume persistente.",
      "en_US" =>
        "⚠️  Generated admin on an ephemeral store: MALACHI_RA_DATA_DIR is unset, so the ra log lives in " <>
          "a temp directory that does not survive a recreated container or a reboot. When it is lost the " <>
          "generated admin goes with it and a new password is generated and logged. Point " <>
          "MALACHI_RA_DATA_DIR at a persistent volume."
    },
    # Validator translations
    # Atom monitor translations
    atom_monitor_started: %{
      "pt_BR" => "✅ AtomMonitor iniciado (intervalo: %{interval_ms}ms, alerta: %{warning}%, crítico: %{critical}%)",
      "en_US" => "✅ AtomMonitor started (interval: %{interval_ms}ms, warning: %{warning}%, critical: %{critical}%)"
    },
    # Memory monitor translations
    memory_monitor_started: %{
      "pt_BR" =>
        "✅ MemoryMonitor iniciado (intervalo: %{interval_ms}ms, GC threshold: %{gc_threshold_mb}MB, auto-GC: %{auto_gc})",
      "en_US" =>
        "✅ MemoryMonitor started (interval: %{interval_ms}ms, GC threshold: %{gc_threshold_mb}MB, auto-GC: %{auto_gc})"
    },
    # UserStore translations
    user_store_persist_error: %{
      "pt_BR" => "❌ Erro de persistência do user store: %{reason}",
      "en_US" => "❌ User store persistence error: %{reason}"
    },
    admin_password_generated: %{
      "pt_BR" =>
        "\n════════════════════════════════════════════════════════════════\n" <>
          "Uma senha de admin aleatória foi gerada no primeiro boot. ANOTE AGORA:\n" <>
          "ela é mostrada só uma vez e não pode ser recuperada:\n\n" <>
          "    usuário: %{username}\n    senha:   %{password}\n\n" <>
          "Defina MALACHI_ADMIN_PASS para usar a sua própria e pular a geração.\n" <>
          "════════════════════════════════════════════════════════════════",
      "en_US" =>
        "\n════════════════════════════════════════════════════════════════\n" <>
          "A random admin password was generated on first boot. SAVE IT NOW:\n" <>
          "it is shown only once and cannot be recovered:\n\n" <>
          "    username: %{username}\n    password: %{password}\n\n" <>
          "Set MALACHI_ADMIN_PASS to provide your own and skip generation.\n" <>
          "════════════════════════════════════════════════════════════════"
    },
    # Retention: data a consumer was moved past (Malachi.Retention.SkipReporter)
    retention_expire_call_failed: %{
      "pt_BR" =>
        "⚠️ o plano de controle não respondeu ao delete do segmento %{segment} (%{reason}); os bytes " <>
          "foram mantidos e a próxima varredura tenta de novo",
      "en_US" =>
        "⚠️ the control plane did not answer the delete of segment %{segment} (%{reason}); the bytes " <>
          "were kept and the next sweep tries again"
    },
    retention_policies_unreadable: %{
      "pt_BR" =>
        "⚠️ varredura de retenção pulada: o store de políticas não respondeu (%{reason}); expirar sob o " <>
          "limite global apagaria justamente o que a política guarda",
      "en_US" =>
        "⚠️ retention sweep skipped: the policy store did not answer (%{reason}); expiring under the " <>
          "global limit would delete exactly what the policy keeps"
    },
    retention_orphan_removed: %{
      "pt_BR" => "🧹 varredura de órfãos recuperou %{count} diretórios de réplica: %{directories}",
      "en_US" => "🧹 the orphan sweep reclaimed %{count} replica directories: %{directories}"
    },
    retention_orphan_remove_failed: %{
      "pt_BR" => "⚠️ varredura de órfãos não conseguiu remover %{failures}; a próxima passada tenta de novo",
      "en_US" => "⚠️ the orphan sweep could not remove %{failures}; the next pass tries again"
    },
    retention_orphan_undecided: %{
      "pt_BR" =>
        "varredura de órfãos mantém %{count} diretórios que ninguém pôde confirmar (sem dono para perguntar, " <>
          "split em curso ou gravados num vnode que não é o dono): %{directories}",
      "en_US" =>
        "the orphan sweep keeps %{count} directories nobody could vouch for (no owner to ask, a split in " <>
          "flight, or written to a vnode that does not own them): %{directories}"
    },
    retention_orphan_authority_unavailable: %{
      "pt_BR" =>
        "varredura de órfãos em %{directory} parada: não obteve uma resposta completa do control plane " <>
          "(%{reason}); sem ela um diretório vivo pareceria órfão",
      "en_US" =>
        "the orphan sweep of %{directory} is holding: it did not get a complete answer from the control " <>
          "plane (%{reason}); without one a live directory would look orphaned"
    },
    retention_orphan_tracking_capped: %{
      "pt_BR" =>
        "⚠️ varredura de órfãos passou de %{limit} candidatos e parou de contar o excedente; a remoção " <>
          "deles só atrasa",
      "en_US" =>
        "⚠️ the orphan sweep passed %{limit} candidates and stopped counting the rest; their removal is " <>
          "only delayed"
    },
    retention_consumer_skipped: %{
      "pt_BR" =>
        "⚠️ O grupo %{group} no tópico %{topic} foi movido além de %{offsets} offsets que não estão mais " <>
          "armazenados (range %{range}, range de origem %{source_range}, origem %{origin}, extensão %{span}); " <>
          "%{held} outros pulos deste leitor foram contados sem linha desde a última",
      "en_US" =>
        "⚠️ Group %{group} on topic %{topic} was moved past %{offsets} offsets that are no longer stored " <>
          "(range %{range}, source range %{source_range}, origin %{origin}, span %{span}); " <>
          "%{held} more skips of this reader were counted without a line since the last one"
    }
  }

  @doc """
  Returns the current locale.
  """
  @spec locale() :: String.t()
  def locale do
    Application.get_env(:malachi, :locale, "en_US")
  end

  @doc """
  Sets the locale at runtime.
  """
  @spec set_locale(String.t()) :: :ok
  def set_locale(new_locale) when new_locale in ["pt_BR", "en_US"] do
    Application.put_env(:malachi, :locale, new_locale)
    :ok
  end

  @doc """
  Translates a key with optional interpolation.

  ## Examples

      iex> Malachi.I18n.t(:metrics_started)
      "✅ Metrics system started"

      iex> Malachi.I18n.t(:transport_enabled, transport: "TLS", port: 4040)
      "🔒 TLS transport enabled on port 4040"
  """
  @spec t(atom(), keyword()) :: String.t()
  def t(key, bindings \\ [])

  def t(key, bindings) when is_atom(key) do
    current_locale = locale()

    case Map.get(@translations, key) do
      nil ->
        to_string(key)

      translations ->
        template = Map.get(translations, current_locale) || Map.get(translations, "en_US") || to_string(key)
        interpolate(template, bindings)
    end
  end

  @doc """
  Lists all available locales.
  """
  @spec available_locales() :: [String.t()]
  def available_locales, do: ["pt_BR", "en_US"]

  @doc """
  Lists all translation keys.
  """
  @spec keys() :: [atom()]
  def keys, do: Map.keys(@translations)

  defp interpolate(template, []), do: template

  defp interpolate(template, bindings) do
    Enum.reduce(bindings, template, fn {key, value}, acc ->
      String.replace(acc, "%{#{key}}", to_string(value))
    end)
  end
end
