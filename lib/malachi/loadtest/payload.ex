defmodule Malachi.Loadtest.Payload do
  @moduledoc """
  The record values the load generators send, as one of three modes chosen by `--payload`:

    * `:constant` (default) - every value is the byte `x` repeated. The series published so far were all
      measured with it, which is the only reason it stays the default: a run of one byte compresses to
      almost nothing, so no compression figure taken with it means anything.
    * `:json` - a seeded, event-like JSON document of exactly `record_size` bytes (see below).
    * `:random` - seeded uniform bytes, the incompressible control (ratio about 1.00).

  `scripts/lib/payload.js` implements the same algorithm, the same document and the same pool, so the same
  mode, seed and size give byte-identical values in both generators. Golden vectors shared by both test
  suites (`test/support/fixtures/loadtest/payload_vectors.json`) pin that.

  ## The generator

  xoshiro128** (Blackman and Vigna, "Scrambled linear pseudorandom number generators", 2018), whose state
  is four 32-bit words. It needs only 32-bit shifts, rotations and multiplications by 5 and 9, which both
  languages can do exactly: here every step is masked to 32 bits, and the JavaScript side uses
  `Math.imul` and `>>> 0`. The 32-bit seed is expanded into the state with SplitMix64, the seeding the
  algorithm's authors recommend: two SplitMix64 outputs, each split into its low and high 32-bit words,
  give `s0`, `s1`, `s2` and `s3` in that order. A bounded draw is `rem(next, n)`; its modulo bias is
  irrelevant here and identical in both languages, which is what matters.

  ## The JSON document

  ASCII only, keys in this order, no whitespace:

      {"id":"<16 hex>","ts":<13 digits>,"type":"<1 of 8>","user":"u<0000..9999>",
       "region":"<1 of 6>","status":"<1 of 4>","amount":<100..99999>,"msg":"<words>"}

  `id` is two draws in lowercase hex (unique in practice, like an event id). `ts` grows with the value's
  position in the pool from a fixed base, plus 0 to 6 ms, never from the clock, so a value depends on the
  seed alone. `msg` is words drawn from a 64-word vocabulary, separated by spaces, and filled until the
  document has exactly `record_size` bytes, cutting the last word. A size below `min_json_size/0` cannot
  hold the fields and is refused rather than shortened.

  The cardinalities are what decide the compression ratio, and the block size (the records per produce,
  `--batch`) decides how much of that redundancy one block can see. With zstd level 1 at 256-byte values
  the document lands near the reference band measured in #186 on JSON of about 200 bytes (1.17x at one
  record per block, 2.6x at 10, 3.8x at 100, 4.1x at 1000); the test suite pins it inside that band.
  Measured on Linux with seed 1: 1.32x, 2.66x, 3.55x and 3.82x at 1, 10, 100 and 1000 records per block.
  Random values give 1.00x from 10 records per block (0.96x for one, the frame's own overhead), and constant
  bytes 13x at one record and 135x at ten.

  ## The pool

  Generating values inside the measured window would make the generator the bottleneck, so the values
  are generated once, before it, into a pool every connection shares. A batch never repeats a value: the
  pool holds whole batches of distinct values. One connection only sends a value again after it has sent
  the whole pool, more than the window zstd uses at its low levels.

  The pool holds `min(ceil(8MiB / record_size), #{65_536})` values, rounded up to whole batches, and at
  least `pipeline` batches for every connection, where `pipeline` is how many produces a connection keeps
  in flight (`--pipeline`, 1 for a closed loop). The cap keeps the pool to 65536 values below 128-byte
  values instead of millions of tiny ones, so there it is smaller than 8MiB (4MiB at 64 bytes).
  Connection `i` starts at batch `i * stride`, with `stride = div(batches, connections)`, which is
  therefore at least `pipeline`, and walks forward on its own cursor with no counter shared between
  connections. So every connection's first burst of `pipeline` batches is its own. Large batches at many
  connections, and deep pipelines, get a larger pool: batch 4096 of 256-byte values at 64 connections
  holds 64MiB, and 2GiB pipelined 32 deep; `Malachi.Loadtest` says so on stderr above 512MiB.

  That is all that is guaranteed between connections. The cursors move at each connection's own pace,
  so a connection that runs `stride` batches ahead of its neighbour reaches the batches the neighbour is
  sending, and from then on both send the batches they share close together, which a store compressing
  several batches at once (#202, #209) sees as repetition. A wider stride, from a larger pool relative to
  the connections, takes longer to close.
  """

  import Bitwise

  alias Malachi.Log.Record
  alias Malachi.Wire

  @modes [:constant, :json, :random]
  @default_seed 1

  @mask32 0xFFFF_FFFF
  @mask64 0xFFFF_FFFF_FFFF_FFFF

  @pool_target_bytes 8 * 1024 * 1024
  @pool_max_values 65_536

  @ts_base 1_700_000_000_000
  @types ~w(order.created order.paid order.shipped order.cancelled cart.updated user.signup user.login page.view)
  @regions ~w(us-east us-west eu-west eu-central ap-south sa-east)
  @statuses ~w(ok pending failed retry)
  @words ~w(
    the a an to of and in on for with from by at as is was are be been has have had not but or if then
    when this that order payment item cart user account session request service client server queue
    stream event record batch retry timeout error update create delete shipped pending checkout
    price total amount region warehouse status delivery customer refund
  )

  # The tuples the hot loop indexes, built once.
  @types_t List.to_tuple(@types)
  @regions_t List.to_tuple(@regions)
  @statuses_t List.to_tuple(@statuses)
  @words_t List.to_tuple(@words)

  if length(@words) != 64, do: raise("the vocabulary must hold 64 words, it holds #{length(@words)}")

  @typedoc "A payload mode."
  @type mode :: :constant | :json | :random

  @typedoc "The xoshiro128** state: four 32-bit words."
  @opaque state :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @doc "The payload modes, in the order `--payload` lists them."
  @spec modes() :: [mode()]
  def modes, do: @modes

  @doc "The seed `json` and `random` use when none is given."
  @spec default_seed() :: pos_integer()
  def default_seed, do: @default_seed

  @doc """
  The smallest `record_size` a `json` value fits in: the document with every field at its widest and an
  empty `msg`.
  """
  @spec min_json_size() :: pos_integer()
  def min_json_size do
    widest = fn list -> list |> Enum.map(&byte_size/1) |> Enum.max() end

    byte_size(
      skeleton(
        String.duplicate("f", 16),
        @ts_base + 9_999_999,
        String.duplicate("t", widest.(@types)),
        "u0000",
        String.duplicate("r", widest.(@regions)),
        String.duplicate("s", widest.(@statuses)),
        99_999
      )
    )
  end

  # --- the generator ---

  @doc "The xoshiro128** state for a 32-bit seed, expanded with SplitMix64."
  @spec seed(non_neg_integer()) :: state()
  def seed(seed) when is_integer(seed) and seed >= 0 and seed <= @mask32 do
    {a, x} = splitmix64(seed)
    {b, _x} = splitmix64(x)
    {a &&& @mask32, a >>> 32, b &&& @mask32, b >>> 32}
  end

  defp splitmix64(x) do
    x = x + 0x9E37_79B9_7F4A_7C15 &&& @mask64
    z = x
    z = bxor(z, z >>> 30) * 0xBF58_476D_1CE4_E5B9 &&& @mask64
    z = bxor(z, z >>> 27) * 0x94D0_49BB_1331_11EB &&& @mask64
    {bxor(z, z >>> 31), x}
  end

  @doc "The next 32-bit output and the advanced state."
  @spec next(state()) :: {non_neg_integer(), state()}
  def next({s0, s1, s2, s3}) do
    result = rotl(s1 * 5 &&& @mask32, 7) * 9 &&& @mask32
    t = s1 <<< 9 &&& @mask32
    s2 = bxor(s2, s0)
    s3 = bxor(s3, s1)
    s1 = bxor(s1, s2)
    s0 = bxor(s0, s3)
    s2 = bxor(s2, t)
    s3 = rotl(s3, 11)
    {result, {s0, s1, s2, s3}}
  end

  defp rotl(x, k), do: (x <<< k &&& @mask32) ||| x >>> (32 - k)

  # A draw in 0..n-1.
  defp uniform(state, n) do
    {x, state} = next(state)
    {rem(x, n), state}
  end

  # --- values ---

  @doc """
  The first `count` values of `mode` for `seed` at `record_size` bytes, in pool order. `constant` ignores
  the seed. Raises `ArgumentError` for a `json` size below `min_json_size/0`.
  """
  @spec values(mode(), non_neg_integer(), pos_integer(), non_neg_integer()) :: [binary()]
  def values(:constant, _seed, record_size, count), do: List.duplicate(:binary.copy("x", record_size), count)

  def values(:json, seed, record_size, count) do
    check_json_size!(record_size)
    generate(seed, count, &json_value(&1, &2, record_size))
  end

  def values(:random, seed, record_size, count),
    do: generate(seed, count, fn state, _i -> random_value(state, record_size) end)

  @doc "Raises `ArgumentError` when `record_size` cannot hold a `json` value."
  @spec check_json_size!(pos_integer()) :: :ok
  def check_json_size!(record_size) do
    min = min_json_size()

    if record_size < min do
      raise ArgumentError, "json payload needs --record-size >= #{min}, got #{record_size}"
    end

    :ok
  end

  defp generate(seed, count, fun) do
    {values, _state} =
      Enum.map_reduce(0..(count - 1)//1, seed(seed), fn i, state ->
        {value, state} = fun.(state, i)
        {value, state}
      end)

    values
  end

  defp json_value(state, index, size) do
    {hi, state} = next(state)
    {lo, state} = next(state)
    {jitter, state} = uniform(state, 7)
    {type, state} = uniform(state, tuple_size(@types_t))
    {user, state} = uniform(state, 10_000)
    {region, state} = uniform(state, tuple_size(@regions_t))
    {status, state} = uniform(state, tuple_size(@statuses_t))
    {amount, state} = uniform(state, 99_900)

    head =
      skeleton(
        hex8(hi) <> hex8(lo),
        @ts_base + index * 7 + jitter,
        elem(@types_t, type),
        "u" <> String.pad_leading(Integer.to_string(user), 4, "0"),
        elem(@regions_t, region),
        elem(@statuses_t, status),
        100 + amount
      )

    # The skeleton ends with `"msg":""}`; the words go between the last two quotes.
    room = size - byte_size(head)
    {msg, state} = words(state, room, [], 0)
    prefix = binary_part(head, 0, byte_size(head) - 2)
    {prefix <> msg <> "\"}", state}
  end

  defp skeleton(id, ts, type, user, region, status, amount) do
    ~s({"id":"#{id}","ts":#{ts},"type":"#{type}","user":"#{user}","region":"#{region}",) <>
      ~s("status":"#{status}","amount":#{amount},"msg":""})
  end

  defp hex8(x), do: x |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(8, "0")

  # Words separated by spaces until `room` bytes are covered, then cut to exactly `room`.
  defp words(state, room, acc, len) when len >= room do
    msg = acc |> Enum.reverse() |> IO.iodata_to_binary()
    {binary_part(msg, 0, room), state}
  end

  defp words(state, room, acc, len) do
    {w, state} = uniform(state, tuple_size(@words_t))
    word = elem(@words_t, w)

    if acc == [] do
      words(state, room, [word], byte_size(word))
    else
      words(state, room, [word, " " | acc], len + 1 + byte_size(word))
    end
  end

  # Four bytes per draw, big-endian, the last draw cut to fit.
  defp random_value(state, size) do
    draws = div(size + 3, 4)

    {chunks, state} =
      Enum.map_reduce(1..draws, state, fn _, state ->
        {x, state} = next(state)
        {<<x::32>>, state}
      end)

    {binary_part(IO.iodata_to_binary(chunks), 0, size), state}
  end

  # --- the pool ---

  @doc """
  How many values the pool holds for `record_size`, `batch`, `connections` and the `pipeline` depth each
  connection keeps in flight (1 for a closed loop): see the moduledoc. Always a whole number of batches,
  and at least `pipeline` batches per connection.
  """
  @spec pool_size(pos_integer(), pos_integer(), pos_integer(), pos_integer()) :: pos_integer()
  def pool_size(record_size, batch, connections, pipeline) do
    wanted = min(div(@pool_target_bytes + record_size - 1, record_size), @pool_max_values)
    batches = max(div(wanted + batch - 1, batch), connections * pipeline)
    batches * batch
  end

  @doc """
  The pool for `mode`, as a tuple of batches already encoded by `Malachi.Wire.encode_produce_records/1`,
  so a produce only joins one to its topic. Value `pos` of the pool is keyed `k<rem(pos, keys)>`. The tuple
  is meant to be shared rather than handed to every connection: `Malachi.Loadtest` keeps it in
  `:persistent_term` for the run, since a term sent or captured into a process is copied there, small
  batches included.
  """
  @spec encoded_batches(
          mode(),
          non_neg_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) :: tuple()
  def encoded_batches(mode, seed, record_size, batch, keys, connections, pipeline) do
    mode
    |> values(seed, record_size, pool_size(record_size, batch, connections, pipeline))
    |> Enum.with_index()
    |> Enum.map(fn {value, pos} -> %Record{value: value, key: "k#{rem(pos, keys)}", timestamp: 0, headers: []} end)
    |> Enum.chunk_every(batch)
    |> Enum.map(&Wire.encode_produce_records/1)
    |> List.to_tuple()
  end

  @doc "The batch connection `index` starts at, out of `batches`, when `connections` share the pool."
  @spec start_batch(non_neg_integer(), pos_integer(), pos_integer()) :: non_neg_integer()
  def start_batch(index, batches, connections), do: rem(index * max(1, div(batches, connections)), batches)
end
