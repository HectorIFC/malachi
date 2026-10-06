defmodule Malachi.UI.TokenGen.LintTest do
  use ExUnit.Case, async: true

  alias Malachi.UI.TokenGen.Lint

  @moduletag :tmp_dir

  setup %{tmp_dir: root} do
    File.mkdir_p!(Path.join(root, "assets/src/styles"))
    File.mkdir_p!(Path.join(root, "tui/src/theme"))
    %{root: root}
  end

  defp write(root, path, content) do
    full = Path.join(root, path)
    File.mkdir_p!(Path.dirname(full))
    File.write!(full, content)
  end

  defp scan(root),
    do: Lint.scan(root, ["assets/src", "tui/src"], ["assets/src/styles/tokens.css", "tui/src/theme/generated.rs"])

  test "a clean tree passes, and the generated files are exempt by exact path", %{root: root} do
    write(root, "assets/src/styles/tokens.css", ":root { --a: oklch(0.5 0.1 20); --b: #ffffff; }\n")
    write(root, "tui/src/theme/generated.rs", "pub const A: Color = Color::Rgb(1, 2, 3);\n")
    write(root, "assets/src/App.tsx", "export const App = () => <div className=\"bg-background\" />;\n")
    assert scan(root) == []
  end

  test "an exemption is the exact file, not anything that looks like it", %{root: root} do
    write(root, "assets/src/styles/tokens.copy.css", ":root { --b: #ffffff; }\n")
    write(root, "assets/src/other/tokens.css", ":root { --b: #ffffff; }\n")
    assert length(scan(root)) == 2
  end

  test "every raw color form is a hit, with its file and line", %{root: root} do
    write(root, "assets/src/Badge.tsx", """
    const a = { color: '#fff' };
    const b = "#0a0e27";
    const c = `oklch(0.5 0.1 20)`;
    // was #00d9ff
    const d = [#abc, x];
    """)

    write(root, "assets/src/badge.css", """
    .x { background:#1a1f3a; }
    .y { color: rgb(0 0 0); border-color: RGBA(0, 0, 0, 0.5); }
    .z { color: hsl(0 0% 0%); fill: hsla(0, 0%, 0%, 1); }
    """)

    write(root, "tui/src/ui.rs", """
    let a = Color::Rgb(1, 2, 3);
    let b = Color::Indexed(214);
    let c = Color::Red;
    let d = Color::Reset;
    """)

    hits = scan(root)
    assert {"assets/src/Badge.tsx", 1, "#fff"} in hits
    assert {"assets/src/Badge.tsx", 2, "#0a0e27"} in hits
    assert {"assets/src/Badge.tsx", 3, "oklch("} in hits
    assert {"assets/src/Badge.tsx", 5, "#abc"} in hits
    assert {"assets/src/badge.css", 1, "#1a1f3a"} in hits
    assert {"assets/src/badge.css", 2, "rgb("} in hits
    assert {"assets/src/badge.css", 2, "RGBA("} in hits
    assert {"assets/src/badge.css", 3, "hsl("} in hits
    assert {"assets/src/badge.css", 3, "hsla("} in hits
    assert {"tui/src/ui.rs", 1, "Color::Rgb"} in hits
    assert {"tui/src/ui.rs", 2, "Color::Indexed"} in hits
    assert {"tui/src/ui.rs", 3, "Color::Red"} in hits
    assert {"tui/src/ui.rs", 4, "Color::Reset"} in hits
  end

  test "a hex in any position of a declaration value or of a string is a color", %{root: root} do
    write(root, "assets/src/card.css", """
    .x { border: 1px solid #ff0000; }
    .y { box-shadow: 0 0 4px #000, inset 0 1px 0 #ffffff80; }
    .z { background: url(a.png) no-repeat #fff; }
    """)

    write(root, "assets/src/Card.tsx", """
    const a = { border: "1px solid #f00" };
    const b = `0 0 2px #123456`;
    const c = 'see #227';
    """)

    hits = scan(root)
    assert {"assets/src/card.css", 1, "#ff0000"} in hits
    assert {"assets/src/card.css", 2, "#000"} in hits
    assert {"assets/src/card.css", 2, "#ffffff80"} in hits
    assert {"assets/src/card.css", 3, "#fff"} in hits
    assert {"assets/src/Card.tsx", 1, "#f00"} in hits
    assert {"assets/src/Card.tsx", 2, "#123456"} in hits
    assert {"assets/src/Card.tsx", 3, "#227"} in hits
  end

  test "a value or a template literal that continues on the next line is still scanned", %{root: root} do
    write(root, "assets/src/shadow.css", """
    .x {
      box-shadow: 0 0 4px #000,
        inset 0 1px 0 #fff;
      background: linear-gradient(
        to right,
        #00ff00 100%
      );
    }
    """)

    write(root, "assets/src/Styled.tsx", """
    const C = styled.div`
      border: 1px solid #f00;
    `;
    """)

    write(root, "tui/src/strings.rs", ~s(let tint = "#ff0000";\n))

    hits = scan(root)
    assert {"assets/src/shadow.css", 2, "#000"} in hits
    assert {"assets/src/shadow.css", 3, "#fff"} in hits
    assert {"assets/src/shadow.css", 6, "#00ff00"} in hits
    assert {"assets/src/Styled.tsx", 2, "#f00"} in hits
    assert {"tui/src/strings.rs", 1, "#ff0000"} in hits
  end

  test "an apostrophe in a comment or a Rust lifetime does not open a string", %{root: root} do
    write(root, "assets/src/Notes.tsx", """
    // it's tracked in #227
    // don't touch until #194
    /* it's the same in a block
       comment until #231 */
    const a = 'ok'; // it's #233 now
    """)

    write(root, "tui/src/life.rs", "fn f<'a>() {} // see #233\nfn g<'a>(x: &'a str) {} // see #231\n")
    assert scan(root) == []
  end

  test "a quote string left open ends with its line instead of swallowing the next", %{root: root} do
    write(root, "assets/src/Open.tsx", "const s = \"open\n// see #227\n")
    assert scan(root) == []
  end

  test "a color after a colon in a comment still counts", %{root: root} do
    write(root, "assets/src/Old.tsx", "// was: #ff0000\n")
    write(root, "tui/src/old.rs", "/* previous tint: #123456 */\n")
    assert [{"assets/src/Old.tsx", 1, "#ff0000"}, {"tui/src/old.rs", 1, "#123456"}] = scan(root)
  end

  test "an escaped hash inside a string is still a color, and does not break the scan", %{root: root} do
    write(root, "assets/src/Esc.tsx", ~S(const s = "\#fff";) <> "\n")
    assert [{"assets/src/Esc.tsx", 1, "#fff"}] = scan(root)
  end

  test "a selector nested in an at-rule or a rule is never a declaration", %{root: root} do
    write(root, "assets/src/nested.css", """
    @media (min-width: 600px) {
      a:hover, #add { color: #ff0000; }
    }
    """)

    write(root, "assets/src/nested2.css", """
    .a {
      .b:hover, #bad {
        border: 1px solid #00ff00;
      }
    }
    """)

    hits = scan(root)
    assert {"assets/src/nested.css", 2, "#ff0000"} in hits
    assert {"assets/src/nested2.css", 3, "#00ff00"} in hits
    refute Enum.any?(hits, fn {_file, _line, match} -> match in ["#add", "#bad"] end)
  end

  test "a Sass or Less stylesheet is a hit of its own, never half scanned", %{root: root} do
    write(root, "assets/src/vars.scss", ".a { color: red; }\n")
    write(root, "assets/src/vars.less", "@primary: #00ff00;\n")
    write(root, "assets/src/indented.sass", ".a\n  color: #0000ff\n")

    assert scan(root) == [
             {"assets/src/indented.sass", 0, "unsupported stylesheet dialect"},
             {"assets/src/vars.less", 0, "unsupported stylesheet dialect"},
             {"assets/src/vars.scss", 0, "unsupported stylesheet dialect"}
           ]
  end

  test "a Sass or Less style block in a component file, and an upper case extension, are hits too", %{root: root} do
    write(root, "assets/src/Upper.SCSS", ".a { color: red; }\n")

    write(root, "assets/src/Card.vue", """
    <template><div class="a" /></template>
    <style lang="scss">
    .a { border: 1px solid #00ff00; }
    </style>
    """)

    write(root, "assets/src/Plain.vue", """
    <style>
    .a { border: 1px solid #0000ff; }
    </style>
    """)

    hits = scan(root)
    assert {"assets/src/Upper.SCSS", 0, "unsupported stylesheet dialect"} in hits
    assert {"assets/src/Card.vue", 0, "unsupported stylesheet dialect"} in hits
    refute Enum.any?(hits, &(elem(&1, 0) == "assets/src/Plain.vue" and elem(&1, 2) == "unsupported stylesheet dialect"))
  end

  test "a script that mentions a Sass style block is still scanned as a script", %{root: root} do
    write(root, "assets/src/Panel.tsx", """
    // Unlike a Vue <style lang="scss"> block, styles here come from tokens.
    export const c = "#ff0000";
    """)

    assert scan(root) == [{"assets/src/Panel.tsx", 2, "#ff0000"}]
  end

  test "a component with a Sass block reports the block and its other colors too", %{root: root} do
    write(root, "assets/src/Badge.vue", """
    <script>export const tint = "#123456";</script>
    <style lang="less">.a { color: red; }</style>
    """)

    assert scan(root) == [
             {"assets/src/Badge.vue", 0, "unsupported stylesheet dialect"},
             {"assets/src/Badge.vue", 1, "#123456"}
           ]
  end

  test "a value that runs to the end of the file without a semicolon still counts", %{root: root} do
    write(root, "assets/src/eof.css", ".a { color: #abcdef")
    assert [{"assets/src/eof.css", 1, "#abcdef"}] = scan(root)
  end

  test "an id selector after a pseudo class is not a declaration", %{root: root} do
    write(root, "assets/src/sel.css", "a:hover, #add { color: red; }\n.b { }\n#bad { margin: 0; }\n")
    assert scan(root) == []
  end

  test "every CSS color function is a hit, color() only with a color space", %{root: root} do
    write(root, "assets/src/fn.css", """
    .a { color: lab(50% 40 59); }
    .b { color: lch(50% 40 59); }
    .c { color: oklab(0.5 0.1 0.1); }
    .d { color: hwb(20 10% 10%); }
    .e { color: color(display-p3 1 0 0); }
    """)

    write(root, "assets/src/helper.ts", "export const shade = theme.color(name);\n")

    hits = scan(root)

    for {line, match} <- [{1, "lab("}, {2, "lch("}, {3, "oklab("}, {4, "hwb("}, {5, "color(display-p3"}] do
      assert {"assets/src/fn.css", line, match} in hits
    end

    refute Enum.any?(hits, &(elem(&1, 0) == "assets/src/helper.ts"))
  end

  test "a symbolic link under a scanned directory is a hit, not a silent skip", %{root: root} do
    vendor = Path.join(root, "vendor/ui")
    File.mkdir_p!(vendor)
    File.write!(Path.join(vendor, "x.tsx"), "const a = { color: '#ff0000' };\n")
    File.ln_s!(vendor, Path.join(root, "assets/src/components"))

    assert [{"assets/src/components", 0, "symbolic link"}] = scan(root)
  end

  test "only interface sources are read, and an escaped quote does not end a string", %{root: root} do
    File.write!(Path.join(root, "assets/src/logo.svg"), ~s(<rect fill="#0a0e27"/>\n))
    File.write!(Path.join(root, "assets/src/notes.md"), "color: #fff\n")
    write(root, "assets/src/Quote.tsx", ~S(const q = "a \" b #ff0000";) <> "\n")
    assert [{"assets/src/Quote.tsx", 1, "#ff0000"}] = scan(root)
  end

  test "a comment counts when the color sits in a color position", %{root: root} do
    write(root, "assets/src/a.css", "/* old: #ffffff */\n")
    assert [{"assets/src/a.css", 1, "#ffffff"}] = scan(root)
  end

  test "an issue reference, a selector and a hash in prose are not colors", %{root: root} do
    write(root, "assets/src/Retention.tsx", """
    const note = 'Per topic retention exists but no API reaches it yet (#194).';
    const other = "lands with the console and the terminal (#231, #233)";
    // see (#275) and #227, fixed in #194
    const label = "Damaged"; // was red before #194
    """)

    write(root, "tui/src/contract.rs", "// The native contract test lands with the TUI project (#231, #233).\n")

    write(root, "assets/src/a.css", "#root { display: grid; }\n#app, #add { margin: 0; }\n")
    write(root, "tui/src/palette.rs", "let mode = theme.palette.background; // Color::from_token\n")
    assert scan(root) == []
  end

  test "a scanned directory that does not exist is an error, not an empty pass", %{root: root} do
    File.rm_rf!(Path.join(root, "tui/src"))
    assert [{"tui/src", 0, "missing directory"}] = scan(root)
  end

  test "describes a hit for a person" do
    assert Lint.describe({"assets/src/a.css", 3, "#fff"}) ==
             "assets/src/a.css:3: raw color literal #fff; use a token from docs/design/design-tokens.json"

    assert Lint.describe({"tui/src", 0, "missing directory"}) ==
             "tui/src: the raw color literal check scans this directory, and it does not exist"

    assert Lint.describe({"assets/src/a.scss", 0, "unsupported stylesheet dialect"}) ==
             "assets/src/a.scss: a Sass or Less stylesheet, which the raw color literal check does not read; " <>
               "the console is shadcn/ui on Tailwind v4 with no preprocessor in its design, so write CSS or decide on one explicitly"

    assert Lint.describe({"assets/src/components", 0, "symbolic link"}) ==
             "assets/src/components: a symbolic link, which the raw color literal check does not follow; " <>
               "keep interface sources as real files under the scanned directories"
  end
end
