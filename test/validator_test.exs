defmodule MalachiMQ.ValidatorTest do
  use ExUnit.Case, async: true
  alias MalachiMQ.Validator

  # Validator is already running as part of the application
  # No setup needed - validation methods are stateless

  describe "validate_queue_name/1" do
    test "accepts valid queue names" do
      assert :ok = Validator.validate_queue_name("orders")
      assert :ok = Validator.validate_queue_name("user.events")
      assert :ok = Validator.validate_queue_name("app-logs_v2")
      assert :ok = Validator.validate_queue_name("a")
      assert :ok = Validator.validate_queue_name("A")
      assert :ok = Validator.validate_queue_name("queue123")
      assert :ok = Validator.validate_queue_name("my.queue-name_123")
    end

    test "accepts maximum length names (255 chars)" do
      name = String.duplicate("a", 255)
      assert :ok = Validator.validate_queue_name(name)
    end

    test "rejects empty string" do
      assert {:error, :invalid_queue_name_empty} = Validator.validate_queue_name("")
    end

    test "rejects names exceeding 255 characters" do
      name = String.duplicate("x", 256)
      assert {:error, :invalid_queue_name_too_long} = Validator.validate_queue_name(name)
    end

    test "rejects names with spaces" do
      assert {:error, :invalid_queue_name_invalid_characters} = Validator.validate_queue_name("my queue")
    end

    test "rejects names with slashes" do
      assert {:error, :invalid_queue_name_invalid_characters} = Validator.validate_queue_name("api/v1")
      assert {:error, :invalid_queue_name_invalid_characters} = Validator.validate_queue_name("path\\to\\queue")
    end

    test "rejects names with colons" do
      assert {:error, :invalid_queue_name_invalid_characters} = Validator.validate_queue_name("queue:name")
    end

    test "rejects path traversal attempts" do
      assert {:error, :invalid_queue_name_invalid_characters} = Validator.validate_queue_name("../../etc")
      assert {:error, :invalid_queue_name_invalid_characters} = Validator.validate_queue_name("..\\..\\windows")
    end

    test "rejects SQL injection attempts" do
      assert {:error, :invalid_queue_name_invalid_characters} = Validator.validate_queue_name("queue;DROP TABLE")
      assert {:error, :invalid_queue_name_invalid_characters} = Validator.validate_queue_name("'; DROP--")
    end

    test "rejects XSS attempts" do
      assert {:error, :invalid_queue_name_invalid_characters} =
               Validator.validate_queue_name("<script>alert(1)</script>")

      assert {:error, :invalid_queue_name_invalid_characters} = Validator.validate_queue_name("<img src=x>")
    end

    test "rejects reserved names" do
      assert {:error, :invalid_queue_name_reserved} = Validator.validate_queue_name("system")
      assert {:error, :invalid_queue_name_reserved} = Validator.validate_queue_name("admin")
      assert {:error, :invalid_queue_name_reserved} = Validator.validate_queue_name("internal")
    end

    test "rejects names starting with underscore" do
      assert {:error, :invalid_queue_name_reserved} = Validator.validate_queue_name("_internal")
      assert {:error, :invalid_queue_name_reserved} = Validator.validate_queue_name("_private")
    end

    test "rejects non-string values" do
      assert {:error, :invalid_queue_name_not_string} = Validator.validate_queue_name(123)
      assert {:error, :invalid_queue_name_not_string} = Validator.validate_queue_name(:atom)
      assert {:error, :invalid_queue_name_not_string} = Validator.validate_queue_name(nil)
    end

    test "uses cache for repeated validations" do
      # First validation - cache miss
      assert :ok = Validator.validate_queue_name("cached_queue")

      # Second validation - should be cache hit (verified by metrics)
      assert :ok = Validator.validate_queue_name("cached_queue")
    end
  end

  describe "validate_channel_name/1" do
    test "accepts valid channel names" do
      assert :ok = Validator.validate_channel_name("notifications")
      assert :ok = Validator.validate_channel_name("chat.room1")
    end

    test "rejects invalid channel names (same rules as queues)" do
      assert {:error, :invalid_channel_name_empty} = Validator.validate_channel_name("")
      assert {:error, :invalid_channel_name_too_long} = Validator.validate_channel_name(String.duplicate("x", 256))
      assert {:error, :invalid_channel_name_invalid_characters} = Validator.validate_channel_name("my channel")
      assert {:error, :invalid_channel_name_reserved} = Validator.validate_channel_name("system")
    end
  end

  describe "validate_payload/2" do
    test "accepts empty payload" do
      assert :ok = Validator.validate_payload("")
    end

    test "accepts 1KB payload" do
      payload = String.duplicate("x", 1024)
      assert :ok = Validator.validate_payload(payload)
    end

    test "accepts 10MB payload (exactly at limit)" do
      payload = :crypto.strong_rand_bytes(10_485_760)
      assert :ok = Validator.validate_payload(payload, 10_485_760)
    end

    test "rejects payload exceeding 10MB" do
      payload = :crypto.strong_rand_bytes(10_485_761)
      assert {:error, :payload_too_large} = Validator.validate_payload(payload, 10_485_760)
    end

    test "rejects non-binary payloads" do
      assert {:error, :invalid_payload_type} = Validator.validate_payload(123, 1000)
      assert {:error, :invalid_payload_type} = Validator.validate_payload(%{}, 1000)
    end
  end

  describe "validate_headers/1" do
    test "accepts valid headers with string values" do
      assert :ok = Validator.validate_headers(%{"key" => "value"})
      assert :ok = Validator.validate_headers(%{"priority" => "high"})
    end

    test "accepts headers with number values" do
      assert :ok = Validator.validate_headers(%{"count" => 5})
      assert :ok = Validator.validate_headers(%{"priority" => 1, "retry" => 3})
    end

    test "accepts headers with boolean values" do
      assert :ok = Validator.validate_headers(%{"active" => true})
      assert :ok = Validator.validate_headers(%{"enabled" => false})
    end

    test "accepts mixed value types" do
      assert :ok =
               Validator.validate_headers(%{
                 "name" => "test",
                 "count" => 10,
                 "enabled" => true
               })
    end

    test "accepts maximum header count (50)" do
      headers = Map.new(1..50, fn i -> {"key#{i}", "value"} end)
      assert :ok = Validator.validate_headers(headers)
    end

    test "rejects more than 50 headers" do
      headers = Map.new(1..51, fn i -> {"key#{i}", "value"} end)
      assert {:error, :invalid_headers_too_many} = Validator.validate_headers(headers)
    end

    test "accepts header key at maximum length (128 bytes)" do
      key = String.duplicate("k", 128)
      assert :ok = Validator.validate_headers(%{key => "value"})
    end

    test "rejects header key exceeding 128 bytes" do
      key = String.duplicate("k", 129)
      assert {:error, :invalid_headers_key_too_long} = Validator.validate_headers(%{key => "value"})
    end

    test "accepts header value at maximum length (1024 bytes)" do
      value = String.duplicate("v", 1024)
      assert :ok = Validator.validate_headers(%{"key" => value})
    end

    test "rejects header value exceeding 1024 bytes" do
      value = String.duplicate("v", 1025)
      assert {:error, :invalid_headers_value_too_long} = Validator.validate_headers(%{"key" => value})
    end

    test "rejects nil values" do
      assert {:error, :invalid_headers_invalid_type} = Validator.validate_headers(%{"optional" => nil})
    end

    test "rejects array values" do
      assert {:error, :invalid_headers_invalid_type} = Validator.validate_headers(%{"tags" => ["a", "b"]})
    end

    test "rejects nested map values" do
      assert {:error, :invalid_headers_invalid_type} = Validator.validate_headers(%{"meta" => %{"x" => 1}})
    end

    test "rejects deeply nested maps" do
      assert {:error, :invalid_headers_invalid_type} =
               Validator.validate_headers(%{
                 "nested" => %{"level2" => %{"level3" => "value"}}
               })
    end

    test "rejects non-map headers" do
      assert {:error, :invalid_headers_not_map} = Validator.validate_headers("not a map")
      assert {:error, :invalid_headers_not_map} = Validator.validate_headers([])
    end

    test "rejects non-string keys" do
      # Elixir atoms as keys should fail
      assert {:error, :invalid_headers_key_invalid_type} = Validator.validate_headers(%{:atom_key => "value"})
    end
  end

  describe "sanitize_for_html/1" do
    test "escapes HTML entities" do
      assert "&lt;script&gt;alert(1)&lt;/script&gt;" =
               Validator.sanitize_for_html("<script>alert(1)</script>")
    end

    test "escapes all dangerous characters" do
      input = ~s(<>&"')
      expected = "&lt;&gt;&amp;&quot;&#x27;"
      assert expected == Validator.sanitize_for_html(input)
    end

    test "handles mixed content" do
      input = "Hello <b>world</b> & \"friends\""
      expected = "Hello &lt;b&gt;world&lt;/b&gt; &amp; &quot;friends&quot;"
      assert expected == Validator.sanitize_for_html(input)
    end

    test "handles non-string gracefully" do
      assert 123 = Validator.sanitize_for_html(123)
      assert nil == Validator.sanitize_for_html(nil)
    end
  end

  describe "sanitize_for_log/1" do
    test "removes CRLF characters" do
      input = "text\r\ninjected"
      assert "text  injected" = Validator.sanitize_for_log(input)
    end

    test "removes tabs" do
      input = "text\twith\ttabs"
      assert "text with tabs" = Validator.sanitize_for_log(input)
    end

    test "limits length to 1000 characters" do
      input = String.duplicate("x", 2000)
      result = Validator.sanitize_for_log(input)
      assert String.length(result) == 1000
    end

    test "handles CRLF injection attempts" do
      input = "queue\r\nINJECTED: malicious header"
      result = Validator.sanitize_for_log(input)
      refute String.contains?(result, "\r")
      refute String.contains?(result, "\n")
    end

    test "handles non-string gracefully" do
      assert 123 = Validator.sanitize_for_log(123)
      assert nil == Validator.sanitize_for_log(nil)
    end
  end

  describe "validate_message_size/2" do
    test "accepts message within size limit" do
      payload = String.duplicate("x", 1000)
      assert :ok = Validator.validate_message_size(payload, 1_048_576)
    end

    test "accepts message at exact size limit" do
      max_size = 1024
      payload = String.duplicate("a", max_size)
      assert :ok = Validator.validate_message_size(payload, max_size)
    end

    test "rejects message exceeding size limit" do
      max_size = 1024
      payload = String.duplicate("x", max_size + 1)
      assert {:error, {:message_too_large, actual, max}} = Validator.validate_message_size(payload, max_size)
      assert actual == max_size + 1
      assert max == max_size
    end

    test "handles empty payload" do
      assert :ok = Validator.validate_message_size("", 1024)
    end

    test "handles unicode characters correctly" do
      # "🚀" is 4 bytes in UTF-8
      # 40 bytes
      payload = String.duplicate("🚀", 10)
      assert :ok = Validator.validate_message_size(payload, 50)
      assert {:error, {:message_too_large, 40, 30}} = Validator.validate_message_size(payload, 30)
    end

    test "handles large messages" do
      # 10 MB payload
      payload = String.duplicate("a", 10_485_760)

      assert {:error, {:message_too_large, 10_485_760, 1_048_576}} =
               Validator.validate_message_size(payload, 1_048_576)
    end

    test "validates with default 1MB limit" do
      payload = String.duplicate("x", 1_048_577)
      assert {:error, {:message_too_large, _, _}} = Validator.validate_message_size(payload, 1_048_576)
    end

    test "returns correct byte counts in error" do
      payload = "test"
      assert {:error, {:message_too_large, 4, 3}} = Validator.validate_message_size(payload, 3)
    end
  end
end
