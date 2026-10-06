defmodule Malachi.UI.TokenGen.Token do
  @moduledoc """
  One token as `Malachi.UI.TokenGen.Source` read it, before anything is computed from it.

    * `path` is where it sits in the token file (`color.state.active`), the name every section of
      the file uses to refer to it.
    * `name` is the name every consumer sees (`state-active`): the CSS custom property without its
      dashes, the key in the snapshot and in `Malachi.UI.Tokens`, and the Rust field once dashes become
      underscores.
    * `raw` is the value as authored: `%{light: _, dark: _}` for a themed color, otherwise the
      `$value`, with references still in their `{path}` form.
    * `ref` is the referenced path when the whole value is one reference, and `nil` otherwise.
    * `modes` are the `$modes` alternatives, in declaration order, each a mode name and a path.
  """

  @enforce_keys [:path, :name, :type, :group, :platforms, :themed, :raw]
  defstruct [:path, :name, :type, :group, :platforms, :themed, :raw, ref: nil, modes: [], reduced_motion: nil]

  @type type ::
          :color
          | :dimension
          | :duration
          | :font_family
          | :font_weight
          | :number
          | :cubic_bezier
          | :shadow
          | :typography

  @type platform :: :web | :elixir | :tui

  @type t :: %__MODULE__{
          path: String.t(),
          name: String.t(),
          type: type(),
          group: String.t(),
          platforms: [platform()],
          themed: boolean(),
          raw: term(),
          ref: String.t() | nil,
          modes: [{String.t(), String.t()}],
          reduced_motion: String.t() | nil
        }
end
