defmodule Pod.Thing do
  use Application
  alias Experimental.Flow

  # TODO: alll the mappings
  # TODO: make the keys regex based
  @mappings %{
    "BSIN" => "bsin",
    "_TYPE_NAME" => "_type_name",
    "_TYPE_CD" => "_type_code", # :manufacturer, :retailer
    "_LINK" => "_url",
    "BRAND" => "brand",
    "Group" => "group",
    "OWNER_CD" => "owner_pod_code",
    "OWNER_CODE" => "owner_pod_code",
    "OWNER_NM" => "owner_name",
    "OWNER_LINK" => "owner_url",
    "OWNER_WIKI_EN" => "owner_wiki_en",
    "BRAND_TYPE_NM" => "brand_type_name",

    "GPC_CD" => "gpc_code",
    "GPC_NM" => "gpc_name",
    "GPC_LEVEL" => "gpc_level",
    "SOURCE" => "release",
    "GPC_C_CD" => "gpc_class_code",
    "GPC_F_CD" => "gpc_family_code",
    "GPC_S_CD" => "gpc_segment_code",

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
        # TODO: debug with IO.inspect
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
