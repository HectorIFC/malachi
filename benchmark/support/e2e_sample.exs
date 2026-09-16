# How an A/B harness reads one end-to-end sample: the produce batch latency line that
# benchmark/throughput_1m.exs prints, for example
#
#     batch latency (1000/batch) us:  p50=1734  p99=3932  max=21442
#
# It lives here, not inside store_error_path_ab.exs, because that script runs on load and so cannot be
# required by a test, and this line is a contract between two scripts: a test pins it against the output
# throughput_1m.exs really prints.
#
#   Code.require_file("support/e2e_sample.exs", __DIR__)

defmodule Malachi.Bench.E2ESample do
  # Anchored to a whole line, so only the produce line throughput_1m.exs prints can match. The consume
  # line says `page latency`, and a line that merely mentions a batch and a p50 does not qualify. The
  # separators are spaces and tabs, never `\s`, which would let a match run across a line break and read
  # a record split over two lines as one sample; a `\r` is allowed only before the line ends.
  @produce_latency ~r/^[ \t]*batch latency \(\d+\/batch\) us:[ \t]+p50=(\d+)[ \t]+p99=(\d+)[ \t]+max=\d+[ \t]*\r?$/m

  @doc """
  The produce p50 and p99, in microseconds, from one throughput_1m.exs output.

  Raises unless exactly one produce latency line is present: none means the run failed before its
  report, and more than one means the output no longer says which number is the sample.
  """
  def parse(output) do
    case Regex.scan(@produce_latency, output, capture: :all_but_first) do
      [[p50, p99]] ->
        %{p50: String.to_integer(p50), p99: String.to_integer(p99)}

      [] ->
        raise "e2e sample without a produce latency line:\n#{output}"

      matches ->
        raise "e2e sample with #{length(matches)} produce latency lines, expected one:\n#{output}"
    end
  end
end
