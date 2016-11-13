defmodule Pod.Thing do
  use Application
  alias Experimental.Flow

  @mappings %{
    "BSIN" => "bsin",
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
    ~r/_ISO/ => "_iso",
    ~R/_DT$/ => "_dt",

    "BRAND" => "brand",
    "OWNER" => "owner",
    "CONTACT" => "contact",
    "CODE" => "code",
    # "Group" => "group", # WARN: might conflict with SQL
    "GPC" => "gpc",
    "GCP" => "gcp",
    "GLN" => "gln",
    "GTIN" => "gtin",
    "SOURCE" => "release",
    "TEL" => "tel",
    "HOTLINE" => "hotline",
    "FAX" => "fax",
    "WEB" => "web",
    "LAST_CHANGE" => "last_change",
    "PARTY" => "party",
    "PROVIDER" => "provider",
    "RETURN" => "return",
    "SEARCH" => "search",
    "SOURCE" => "source",
    "SYNC" => "sync",

    "ADDR_02" => "address_line_1",
    "ADDR_03" => "address_line_2",
    "ADDR_04" => "address_line_3",
    "ADDR_POSTCODE" => "address_postcode",
    "ADDR_CITY" => "address_city",

    "GEPIR" => "gepir", # TODO: try to find this one in the xlsx, not sure what this is

    "COUNTY" => "country",
    "COUNTRY" => "country_iso_code",
    "PREFIX_NM" => "office"
  }

  # TODO: optimize! use Flow or GenStage
  def from_sql(source) do
    File.stream!(source)
    |> Stream.transform("", fn line, sql ->
      replaced = Enum.reduce(@mappings, line, fn {from, to}, acc ->
        String.replace(acc, from, to)
      end)

      {[replaced], sql <> replaced}
    end)
    |> Enum.join("")
  end

  defp parse_args(args) do
    {options, _, _} = OptionParser.parse(args,
      switches: [name: :string]
    )
    options
  end

  def start(_type, args) do
    IO.puts "Converting POD database to Thing database format ..."

    sql = Pod.Thing.from_sql("test.sql")
    # sql = :timer.tc(fn -> Pod.Thing.from_sql("pod.sql") end) |> IO.inspect

    IO.puts "Done!"

    {:ok, self(), sql}
  end

end
