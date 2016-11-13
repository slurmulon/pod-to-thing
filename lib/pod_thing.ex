defmodule Pod.Thing do
  use Application
  alias Experimental.Flow

  # TODO: alll the mappings
  # TODO: make the keys regex based
  @mappings %{
    "BSIN" => "bsin",
    # "_TYPE_NAME" => "_type_name",
    # "_TYPE_CD" => "_type_code", # :manufacturer, :retailer
    ~r/_LINK$/ => "_url",
    ~r/_CD$/ => "_code",
    ~r/_NM$/ => "_name",
    ~r/_TYPE/ => "_type",
    ~r/_GROUP/ => "_group",
    ~r/_LEVEL/ => "_level",
    ~r/_WIKI_EN/ => "_wiki_en",
    ~r/_C_CD/ => "_class_code",
    ~r/_F_CD/ => "_family_code",
    ~r/_S_CD/ => "_segment_code",

    "BRAND" => "brand",
    "OWNER" => "owner",
    "Group" => "group",
    "GPC" => "gpc",
    "SOURCE" => "release",

    "COUNTRY_ISO_CD" => "country_iso_code",
    "PREFIX_NM" => "office"
  }

  # TODO: utilize https://hexdocs.pm/gen_stage/Experimental.Flow.html#partition/2-options
  # FIXME: something is definitely off here, bloats way tooo big in memory
  def from_sql(source) do
    # File.stream!(source, :line)
    File.stream!(source)
    |> Flow.from_enumerable()
    |> Flow.partition()
    |> Flow.reduce(fn -> %{} end, fn line, sql ->
      mappings = Enum.sort(@mappings, fn({from, to}) -> from end)

      Enum.flat_map_reduce(mappings, sql, fn({from, to}, result) ->
        {[String.replace(line, from, to)], result}
      end)
    end)
    |> Enum.sort()
    |> Enum.join("")
    # TODO: IO.binwrite, File.close
  end

  def from_sql_sync(source) do
    File.stream!(source)
    |> Stream.transform("", fn(line, acc) ->
      replaced = Enum.reduce(@mappings, line, fn({from, to}, acc) ->
        String.replace(acc, from, to)
      end)

      {[replaced], acc <> replaced}
    end)
    |> Enum.join("")
    |> IO.inspect
  end

  def start(_type, _args) do
    IO.puts "Converting POD database to Thing database format"

    sql = Pod.Thing.from_sql_sync("test.sql")

    {:ok, self(), sql}
  end

end
