defmodule Malachi.Storage.Layout do
  @moduledoc """
  Where a segment's files live on disk: the mapping from a control-plane `segment_id` to the
  directory holding that segment's log.

  Kept apart from `Malachi.Cluster.ReplicationServer` because more than one component needs the
  same answer without going through the server's loop: the server itself opens and deletes
  segments, while `Malachi.Cluster.Scrubber` reads them to verify their checksums. One shared
  mapping means the scrub can never look in a different place than the writer.
  """

  @doc """
  The directory holding `segment_id` under `base`.

  Broker ids (`{{topic, range_seq}, seg_seq}`) get a readable name, `topic-r<range>-s<segment>`.
  Any other term (or a topic that is not path-safe) falls back to a collision-free Base64 encoding
  of the id, which keeps the directory inside `base`: segment ids arrive over inter-node
  replication, so the topic is not trusted here even though `Malachi.Metadata.valid_topic_name?/1`
  screens locally created topics.
  """
  @spec segment_directory(Path.t(), term()) :: Path.t()
  def segment_directory(base, {{topic, range_seq}, seg_seq} = segment_id)
      when is_integer(range_seq) and is_integer(seg_seq) do
    if safe_path_segment?(topic) do
      Path.join(base, "#{topic}-r#{range_seq}-s#{seg_seq}")
    else
      encoded_segment_directory(base, segment_id)
    end
  end

  def segment_directory(base, segment_id), do: encoded_segment_directory(base, segment_id)

  defp encoded_segment_directory(base, segment_id) do
    Path.join(base, Base.url_encode64(:erlang.term_to_binary(segment_id), padding: false))
  end

  # Mirrors the allowlist in Metadata.valid_topic_name?/1 as a defense-in-depth check where the path
  # is built, since segment_ids can arrive from other nodes.
  defp safe_path_segment?(topic) do
    is_binary(topic) and topic not in ["", ".", ".."] and topic =~ ~r/\A[A-Za-z0-9._-]+\z/
  end
end
