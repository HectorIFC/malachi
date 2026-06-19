/**
 * Malachi Producer - Internationalization (i18n)
 * 
 * Supports Brazilian Portuguese (pt_BR) and American English (en_US)
 * 
 * Usage:
 *   MALACHI_LOCALE=en_US node producer.js
 *   MALACHI_LOCALE=pt_BR node producer.js
 */

const translations = {
  // Producer Header
  producer_title: {
    pt_BR: '🚀 Malachi Producer',
    en_US: '🚀 Malachi Producer',
  },
  producer_title_fast: {
    pt_BR: '🚀 Malachi Producer (Modo Rápido)',
    en_US: '🚀 Malachi Producer (Fast Mode)',
  },
  producer_title_continuous: {
    pt_BR: '🚀 Malachi Producer (Modo Contínuo)',
    en_US: '🚀 Malachi Producer (Continuous Mode)',
  },
  microservice_simulator: {
    pt_BR: 'Malachi Producer - Simulador de Microserviço',
    en_US: 'Malachi Producer - Microservice Simulator',
  },
  
  // Connection
  host: {
    pt_BR: 'Host',
    en_US: 'Host',
  },
  queue: {
    pt_BR: 'Fila',
    en_US: 'Queue',
  },
  messages: {
    pt_BR: 'Mensagens',
    en_US: 'Messages',
  },
  connected: {
    pt_BR: '✓ Conectado ao Malachi',
    en_US: '✓ Connected to Malachi',
  },
  connections_established: {
    pt_BR: '✓ %{count} conexões estabelecidas',
    en_US: '✓ %{count} connections established',
  },
  press_ctrl_c: {
    pt_BR: 'Pressione Ctrl+C para parar',
    en_US: 'Press Ctrl+C to stop',
  },
  
  // Messages
  message_sent: {
    pt_BR: 'Mensagem enviada',
    en_US: 'Message sent',
  },
  message_payload: {
    pt_BR: 'Mensagem #%{id} do Node.js',
    en_US: 'Message #%{id} from Node.js',
  },
  fast_message_payload: {
    pt_BR: 'Mensagem rápida #%{id}',
    en_US: 'Fast message #%{id}',
  },
  continuous_message_payload: {
    pt_BR: 'Mensagem contínua #%{id}',
    en_US: 'Continuous message #%{id}',
  },
  
  // Results
  result: {
    pt_BR: '📊 Resultado:',
    en_US: '📊 Result:',
  },
  success: {
    pt_BR: '✓ Sucesso',
    en_US: '✓ Success',
  },
  errors: {
    pt_BR: '✗ Erros',
    en_US: '✗ Errors',
  },
  time: {
    pt_BR: '⏱ Tempo',
    en_US: '⏱ Time',
  },
  rate: {
    pt_BR: '📈 Taxa',
    en_US: '📈 Rate',
  },
  msgs_per_second: {
    pt_BR: 'msgs/segundo',
    en_US: 'msgs/second',
  },
  total_sent: {
    pt_BR: '📊 Total enviado: %{count} mensagens',
    en_US: '📊 Total sent: %{count} messages',
  },
  
  // Errors
  error: {
    pt_BR: 'Erro',
    en_US: 'Error',
  },
  error_prefix: {
    pt_BR: '❌ Erro',
    en_US: '❌ Error',
  },
  timeout_error: {
    pt_BR: 'Timeout aguardando resposta',
    en_US: 'Timeout waiting for response',
  },
  not_connected_error: {
    pt_BR: 'Não conectado. Chame connect() primeiro.',
    en_US: 'Not connected. Call connect() first.',
  },
  check_server_running: {
    pt_BR: 'Verifique se o Malachi está rodando em %{host}:%{port}',
    en_US: 'Check if Malachi is running at %{host}:%{port}',
  },
  
  // Actions
  stopping: {
    pt_BR: '⏹ Parando...',
    en_US: '⏹ Stopping...',
  },
  
  // Help
  usage: {
    pt_BR: 'Uso:',
    en_US: 'Usage:',
  },
  options: {
    pt_BR: 'Opções:',
    en_US: 'Options:',
  },
  examples: {
    pt_BR: 'Exemplos:',
    en_US: 'Examples:',
  },
  env_variables: {
    pt_BR: 'Variáveis de ambiente:',
    en_US: 'Environment variables:',
  },
  help_show: {
    pt_BR: 'Mostra esta ajuda',
    en_US: 'Show this help',
  },
  help_fast: {
    pt_BR: 'Modo rápido (paralelo)',
    en_US: 'Fast mode (parallel)',
  },
  help_continuous: {
    pt_BR: 'Modo contínuo (1 msg/segundo)',
    en_US: 'Continuous mode (1 msg/second)',
  },
  example_default: {
    pt_BR: '# Envia 10 mensagens',
    en_US: '# Sends 10 messages',
  },
  example_100: {
    pt_BR: '# Envia 100 mensagens',
    en_US: '# Sends 100 messages',
  },
  example_fast: {
    pt_BR: '# Envia 1000 em paralelo',
    en_US: '# Sends 1000 in parallel',
  },
  example_continuous: {
    pt_BR: '# Envia continuamente',
    en_US: '# Sends continuously',
  },
  env_host: {
    pt_BR: 'Host do servidor (default: localhost)',
    en_US: 'Server host (default: localhost)',
  },
  env_port: {
    pt_BR: 'Porta TCP (default: 4040)',
    en_US: 'TCP port (default: 4040)',
  },
  env_queue: {
    pt_BR: 'Nome da fila (default: test)',
    en_US: 'Queue name (default: test)',
  },

  auth_failed_error: {
    pt_BR: 'Falha na autenticação',
    en_US: 'Authentication failed',
  },
  authenticating: {
    pt_BR: 'Autenticando...',
    en_US: 'Authenticating...',
  },
  authenticated: {
    pt_BR: '✓ Autenticado como %{username}',
    en_US: '✓ Authenticated as %{username}',
  },
};

// Get current locale from environment
const currentLocale = process.env.MALACHI_LOCALE || 'pt_BR';

/**
 * Translate a key with optional interpolation
 * @param {string} key - Translation key
 * @param {object} bindings - Values to interpolate
 * @returns {string} Translated string
 */
function t(key, bindings = {}) {
  const translation = translations[key];
  
  if (!translation) {
    return key;
  }
  
  let text = translation[currentLocale] || translation['en_US'] || key;
  
  // Interpolate bindings
  for (const [k, v] of Object.entries(bindings)) {
    text = text.replace(new RegExp(`%{${k}}`, 'g'), v);
  }
  
  return text;
}

/**
 * Get current locale
 * @returns {string} Current locale
 */
function getLocale() {
  return currentLocale;
}

/**
 * List available locales
 * @returns {string[]} Available locales
 */
function availableLocales() {
  return ['pt_BR', 'en_US'];
}

module.exports = { t, getLocale, availableLocales };
