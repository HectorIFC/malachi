# Start the application for all tests
# Individual tests handle their own state isolation via setup/on_exit blocks
{:ok, _} = Application.ensure_all_started(:malachimq)

ExUnit.start()
