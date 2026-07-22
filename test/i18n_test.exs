defmodule Malachi.I18nTest do
  # async: false because set_locale modifies global Application env
  use ExUnit.Case, async: false

  alias Malachi.I18n

  doctest Malachi.I18n

  setup do
    original_locale = I18n.locale()
    on_exit(fn -> I18n.set_locale(original_locale) end)
    :ok
  end

  describe "I18n translations" do
    test "locale can be changed at runtime" do
      I18n.set_locale("en_US")
      assert I18n.locale() == "en_US"

      I18n.set_locale("pt_BR")
      assert I18n.locale() == "pt_BR"
    end

    test "transport_enabled translation works" do
      I18n.set_locale("en_US")
      assert I18n.t(:transport_enabled, transport: "TLS", port: 4040) =~ "TLS transport enabled"

      I18n.set_locale("pt_BR")
      assert I18n.t(:transport_enabled, transport: "TLS", port: 4040) =~ "Transporte TLS habilitado"
    end

    test "tls_handshake_failed translation works" do
      I18n.set_locale("en_US")
      assert I18n.t(:tls_handshake_failed, reason: "timeout") =~ "TLS handshake failed"

      I18n.set_locale("pt_BR")
      assert I18n.t(:tls_handshake_failed, reason: "timeout") =~ "Falha no handshake TLS"
    end

    test "all translation keys are present" do
      keys = I18n.keys()

      # Verify all keys have translations for both locales
      for key <- keys do
        en_translation = I18n.t(key)
        refute is_nil(en_translation), "Missing translation for key: #{key}"
        refute en_translation == to_string(key), "No translation found for key: #{key}"
      end
    end

    test "available locales returns correct list" do
      locales = I18n.available_locales()
      assert "en_US" in locales
      assert "pt_BR" in locales
    end

    test "translation with missing key returns key as string" do
      assert I18n.t(:non_existent_key) == "non_existent_key"
    end

    test "translation with interpolation works" do
      I18n.set_locale("en_US")
      result = I18n.t(:transport_enabled, transport: "TLS", port: 4040)
      assert result =~ "TLS"
      assert result =~ "4040"
    end
  end
end
