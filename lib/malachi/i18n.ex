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
    data_shards_ignored_clustered: %{
      "pt_BR" => "⚠️ MALACHI_DATA_SHARDS é ignorado quando o control plane é clusterizado; usando 1 shard",
      "en_US" => "⚠️ MALACHI_DATA_SHARDS is ignored when the control plane is clustered; using 1 shard"
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
    scrubber_unexpected_message: %{
      "pt_BR" => "scrubber ignorando mensagem inesperada: %{message}",
      "en_US" => "scrubber ignoring unexpected message: %{message}"
    },
    scrubber_invalid_interval: %{
      "pt_BR" =>
        "intervalo de scrub %{interval} não é um número positivo de milissegundos, usando o padrão de %{default}ms",
      "en_US" =>
        "scrub interval %{interval} is not a positive number of milliseconds, using the default of %{default}ms"
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
    heal_orphaned_fence_unrecorded: %{
      "pt_BR" =>
        "não foi possível registrar o seal de %{count} segmento(s) cercado(s): %{segments}. Os ranges " <>
          "deles não aceitam escrita até uma passagem posterior conseguir, então o control plane é o " <>
          "lugar a olhar",
      "en_US" =>
        "could not record the seal for %{count} fenced segment(s): %{segments}. Their ranges take no " <>
          "write until a later pass succeeds, so the control plane is the thing to look at"
    },
    heal_seal_no_majority: %{
      "pt_BR" =>
        "segmento %{segment_id} não pode ser selado para failover: %{answered} de %{replicas} réplicas " <>
          "responderam, sem maioria. Seu range fica bloqueado para escrita até uma maioria voltar, " <>
          "porque selar numa minoria poderia descartar escritas já confirmadas",
      "en_US" =>
        "segment %{segment_id} cannot be sealed for failover: %{answered} of %{replicas} replicas " <>
          "answered, no majority. Its range is blocked for writes until a majority returns, because " <>
          "sealing on a minority could discard acknowledged writes"
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
