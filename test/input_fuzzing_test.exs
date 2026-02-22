defmodule MalachiMQ.InputFuzzingTest do
  @moduledoc """
  Systematic fuzzing of all Validator functions with randomly generated data.

  Unlike injection_attack_test.exs (which tests known attack patterns),
  this file generates random inputs including boundary values, unicode edge cases,
  and random binary to test defensive coding. The key assertion is that
  Validator functions NEVER raise exceptions for any input.
  """
  use ExUnit.Case, async: false

  alias MalachiMQ.Validator
  alias MalachiMQ.Test.SecurityHelper

  @moduletag :security

  # Number of random inputs per fuzz test
  @fuzz_iterations 500

  describe "queue name fuzzing" do
    test "random ASCII strings never crash validate_queue_name" do
      inputs =
        for _ <- 1..@fuzz_iterations do
          len = :rand.uniform(300)
          SecurityHelper.random_ascii_string(len)
        end

      results = SecurityHelper.assert_no_crash(&Validator.validate_queue_name/1, inputs)

      # Every result should be :ok or {:error, atom}
      for {_input, result} <- results do
        assert result == :ok or match?({:error, _}, result),
               "Unexpected result: #{inspect(result)}"
      end
    end

    test "random unicode strings never crash validate_queue_name" do
      inputs =
        for _ <- 1..@fuzz_iterations do
          len = :rand.uniform(300)
          SecurityHelper.random_unicode_string(len)
        end

      SecurityHelper.assert_no_crash(&Validator.validate_queue_name/1, inputs)
    end

    test "empty and boundary length strings" do
      inputs = [
        "",
        "a",
        String.duplicate("a", 254),
        String.duplicate("a", 255),
        String.duplicate("a", 256),
        String.duplicate("a", 1000),
        String.duplicate("a", 10_000)
      ]

      results = SecurityHelper.assert_no_crash(&Validator.validate_queue_name/1, inputs)

      # Empty should be rejected
      empty_result =
        Enum.find_value(results, fn {input, result} ->
          if input == "", do: result
        end)

      assert {:error, :invalid_queue_name_empty} = empty_result

      # 255 chars should be ok
      result_255 = Enum.find(results, fn {input, _} -> byte_size(input) == 255 end)
      assert {_, :ok} = result_255

      # 256 chars should be too long
      result_256 = Enum.find(results, fn {input, _} -> byte_size(input) == 256 end)
      assert {_, {:error, :invalid_queue_name_too_long}} = result_256
    end

    test "non-string types never crash validate_queue_name" do
      non_string_inputs = [
        nil,
        42,
        3.14,
        :an_atom,
        true,
        false,
        [],
        [1, 2, 3],
        %{},
        %{"key" => "value"},
        {:tuple, "value"},
        self(),
        make_ref()
      ]

      results =
        SecurityHelper.assert_no_crash(&Validator.validate_queue_name/1, non_string_inputs)

      # All non-strings should be rejected
      for {_input, result} <- results do
        assert {:error, :invalid_queue_name_not_string} = result
      end
    end

    test "strings with control characters" do
      control_chars =
        for c <- 0..31 do
          "queue_" <> <<c>> <> "_test"
        end

      SecurityHelper.assert_no_crash(&Validator.validate_queue_name/1, control_chars)
    end
  end

  describe "channel name fuzzing" do
    test "random ASCII strings never crash validate_channel_name" do
      inputs =
        for _ <- 1..@fuzz_iterations do
          len = :rand.uniform(300)
          SecurityHelper.random_ascii_string(len)
        end

      results = SecurityHelper.assert_no_crash(&Validator.validate_channel_name/1, inputs)

      for {_input, result} <- results do
        assert result == :ok or match?({:error, _}, result)
      end
    end

    test "non-string types never crash validate_channel_name" do
      non_string_inputs = [nil, 42, 3.14, :atom, true, [], %{}, {:tuple}, self()]

      results =
        SecurityHelper.assert_no_crash(&Validator.validate_channel_name/1, non_string_inputs)

      for {_input, result} <- results do
        assert {:error, :invalid_channel_name_not_string} = result
      end
    end

    test "boundary lengths for channel names" do
      inputs = [
        "",
        "a",
        String.duplicate("c", 255),
        String.duplicate("c", 256),
        String.duplicate("c", 1000)
      ]

      results = SecurityHelper.assert_no_crash(&Validator.validate_channel_name/1, inputs)

      assert {:error, :invalid_channel_name_empty} =
               Enum.find_value(results, fn {input, result} ->
                 if input == "", do: result
               end)
    end
  end

  describe "payload fuzzing" do
    test "random binary payloads never crash validate_payload" do
      sizes = [0, 1, 100, 1024, 10_240, 102_400]

      inputs =
        for size <- sizes do
          if size == 0, do: <<>>, else: SecurityHelper.random_binary(size)
        end

      SecurityHelper.assert_no_crash(fn payload -> Validator.validate_payload(payload) end, inputs)
    end

    test "oversized payload is rejected" do
      # 11MB payload (exceeds default 10MB limit)
      oversized = SecurityHelper.random_binary(11_534_336)

      result = Validator.validate_payload(oversized)
      assert {:error, :payload_too_large} = result
    end

    test "payload at exact boundary" do
      max_size = 1_048_576

      # Exactly at limit
      at_limit = SecurityHelper.random_binary(max_size)
      assert :ok = Validator.validate_payload(at_limit, max_size)

      # One byte over
      over_limit = SecurityHelper.random_binary(max_size + 1)
      assert {:error, :payload_too_large} = Validator.validate_payload(over_limit, max_size)
    end

    test "non-binary types for payload" do
      non_binary_inputs = [nil, 42, :atom, [], %{}, true]

      results =
        SecurityHelper.assert_no_crash(
          fn input -> Validator.validate_payload(input) end,
          non_binary_inputs
        )

      for {_input, result} <- results do
        assert {:error, :invalid_payload_type} = result
      end
    end

    test "validate_message_size with random binaries" do
      inputs =
        for _ <- 1..100 do
          size = :rand.uniform(100_000)
          SecurityHelper.random_binary(size)
        end

      results =
        SecurityHelper.assert_no_crash(
          fn payload -> Validator.validate_message_size(payload, 50_000) end,
          inputs
        )

      for {input, result} <- results do
        if byte_size(input) <= 50_000 do
          assert :ok = result
        else
          assert {:error, {:message_too_large, _, 50_000}} = result
        end
      end
    end
  end

  describe "header fuzzing" do
    test "random string-keyed maps never crash validate_headers" do
      inputs =
        for _ <- 1..@fuzz_iterations do
          key_count = :rand.uniform(10)

          for _ <- 1..key_count, into: %{} do
            key = SecurityHelper.random_ascii_string(:rand.uniform(20))
            value = SecurityHelper.random_ascii_string(:rand.uniform(50))
            {key, value}
          end
        end

      SecurityHelper.assert_no_crash(&Validator.validate_headers/1, inputs)
    end

    test "headers with too many keys are rejected" do
      # 51 keys should exceed the 50-key limit
      big_headers = for i <- 1..51, into: %{}, do: {"key_#{i}", "value_#{i}"}

      result = Validator.validate_headers(big_headers)
      assert {:error, :invalid_headers_too_many} = result
    end

    test "headers with oversized keys are rejected" do
      long_key = String.duplicate("k", 129)
      headers = %{long_key => "value"}

      result = Validator.validate_headers(headers)
      assert {:error, :invalid_headers_key_too_long} = result
    end

    test "headers with oversized values are rejected" do
      long_value = String.duplicate("v", 1025)
      headers = %{"key" => long_value}

      result = Validator.validate_headers(headers)
      assert {:error, :invalid_headers_value_too_long} = result
    end

    test "non-map types for headers" do
      non_map_inputs = [nil, 42, "string", :atom, [], true, {:tuple}]

      results =
        SecurityHelper.assert_no_crash(&Validator.validate_headers/1, non_map_inputs)

      for {_input, result} <- results do
        assert {:error, :invalid_headers_not_map} = result
      end
    end

    test "headers with mixed value types" do
      headers = %{
        "string" => "value",
        "integer" => 42,
        "float" => 3.14,
        "bool" => true
      }

      # Should not crash - may accept or reject depending on implementation
      result = Validator.validate_headers(headers)
      assert result == :ok or match?({:error, _}, result)
    end

    test "headers with nil and complex values" do
      test_cases = [
        %{"key" => nil},
        %{"key" => %{"nested" => "map"}},
        %{"key" => [1, 2, 3]}
      ]

      for headers <- test_cases do
        result = Validator.validate_headers(headers)
        assert result == :ok or match?({:error, _}, result)
      end
    end
  end

  describe "sanitization fuzzing" do
    test "sanitize_for_html never crashes and escapes HTML entities" do
      inputs =
        for _ <- 1..@fuzz_iterations do
          len = :rand.uniform(200)
          SecurityHelper.random_ascii_string(len)
        end

      results =
        SecurityHelper.assert_no_crash(&Validator.sanitize_for_html/1, inputs)

      for {_input, result} <- results do
        assert is_binary(result)
        # Result should never contain raw < or > (they should be escaped)
        refute String.contains?(result, "<"),
               "sanitize_for_html output contains raw '<': #{inspect(result, limit: 100)}"

        refute String.contains?(result, ">"),
               "sanitize_for_html output contains raw '>': #{inspect(result, limit: 100)}"
      end
    end

    test "sanitize_for_html handles known attack strings" do
      payloads = SecurityHelper.owasp_injection_payloads()

      results = SecurityHelper.assert_no_crash(&Validator.sanitize_for_html/1, payloads)

      for {_input, result} <- results do
        assert is_binary(result)
        refute String.contains?(result, "<script>")
        refute String.contains?(result, "<img")
        refute String.contains?(result, "<svg")
      end
    end

    test "sanitize_for_log never crashes and removes CRLF" do
      inputs =
        for _ <- 1..@fuzz_iterations do
          len = :rand.uniform(200)
          base = SecurityHelper.random_ascii_string(len)
          # Inject some CRLF characters
          base <> "\r\n" <> SecurityHelper.random_ascii_string(10)
        end

      results =
        SecurityHelper.assert_no_crash(&Validator.sanitize_for_log/1, inputs)

      for {_input, result} <- results do
        assert is_binary(result)

        refute String.contains?(result, "\r"),
               "sanitize_for_log output contains CR"

        refute String.contains?(result, "\n"),
               "sanitize_for_log output contains LF"
      end
    end

    test "sanitize_for_log truncates to 1000 characters" do
      long_input = String.duplicate("x", 2000)
      result = Validator.sanitize_for_log(long_input)

      assert byte_size(result) <= 1000
    end

    test "sanitize functions handle non-string input gracefully" do
      non_strings = [nil, 42, :atom, [], %{}, true]

      SecurityHelper.assert_no_crash(&Validator.sanitize_for_html/1, non_strings)
      SecurityHelper.assert_no_crash(&Validator.sanitize_for_log/1, non_strings)
    end
  end
end
