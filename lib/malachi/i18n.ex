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
