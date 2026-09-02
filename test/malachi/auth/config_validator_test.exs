defmodule Malachi.Auth.ConfigValidatorTest do
  # async: false: these toggle application env (:generate_admin, :ra_data_dir), which is global.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Malachi.Auth.ConfigValidator

  describe "validate!/1 generated-admin persistence warning" do
    setup do
      # Capture and restore, so a test cannot leak the ephemeral-store combo into the rest of the suite.
      original = {
        Application.get_env(:malachi, :generate_admin),
        Application.get_env(:malachi, :ra_data_dir)
      }

      on_exit(fn ->
        {ga, dir} = original

        restore = fn key, value ->
          if is_nil(value), do: Application.delete_env(:malachi, key), else: Application.put_env(:malachi, key, value)
        end

        restore.(:generate_admin, ga)
        restore.(:ra_data_dir, dir)
      end)

      :ok
    end

    test "warns when the admin is generated and the ra store is on the ephemeral default" do
      Application.put_env(:malachi, :generate_admin, true)
      Application.delete_env(:malachi, :ra_data_dir)

      log = capture_log(fn -> assert :ok = ConfigValidator.validate!(:dev) end)

      assert log =~ "MALACHI_RA_DATA_DIR"
      assert log =~ "ephemeral"
    end

    test "stays silent when a persistent ra data directory is configured" do
      # This is the case every shipped deployment is in (docker-compose, the k8s manifest), so it must
      # not produce noise operators learn to ignore.
      Application.put_env(:malachi, :generate_admin, true)
      Application.put_env(:malachi, :ra_data_dir, "/app/data/ra")

      log = capture_log(fn -> assert :ok = ConfigValidator.validate!(:dev) end)

      refute log =~ "ephemeral"
    end

    test "stays silent when the admin is not generated, even on the ephemeral default" do
      # generate_admin is off in dev/test, so the warning must not fire there regardless of the dir.
      Application.put_env(:malachi, :generate_admin, false)
      Application.delete_env(:malachi, :ra_data_dir)

      log = capture_log(fn -> assert :ok = ConfigValidator.validate!(:dev) end)

      refute log =~ "ephemeral"
    end
  end
end
