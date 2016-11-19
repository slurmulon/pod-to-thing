defmodule Pod.Thing do
  use Application

  # IDEA: could use :lowercase atom so we don't have to do so much error-prone copy pasta
  @mappings %{
    ~r/GS1/im => "gs1",
    ~r/_CD/m => "_code",
    ~r/_NM/m => "_name",
    ~r/PREFIX/i => "office",
    ~r/_TYPE/m => "_type",
    ~r/_GROUP/m => "_group",
    ~r/_LEVEL/m => "_level",
    ~r/_WIKI_EN/m => "_wiki_en",
    ~r/_C_/m => "_class_",
    ~r/_F_/m => "_family_",
    ~r/_S_/m => "_segment_",
    ~r/_B_/m => "_brick_",
    ~r/_ISO/m => "_iso",
    ~r/_DT/m => "_date",
    ~r/_DV/m => "_daily_value",
    ~r/CAL(?!CIUM)/m => "calories",

    "ADD_PARTY_ID" => :lower,
    "ADDR_02" => "address_line_1",
    "ADDR_03" => "address_line_2",
    "ADDR_04" => "address_line_3",
    "ADDR_POSTCODE" => "address_postcode",
    "ADDR_CITY" => "address_city",
    "BSIN" => :lower,
    "BRAND" => :lower,
    "CALCIUM" => :lower,
    "CARB" => :lower,
    "CHOL" => "cholesterol",
    "CONTACT" => :lower,
    "CODE" => :lower,
    "COUNTY" => :lower,
    "COUNTRY" => :lower,
    "DESC" => :lower,
    "DIET" => :lower,
    "FAT" => :lower,
    "FIBER" => :lower,
    # "Group" => "group", # WARN: might conflict with SQL
    "GEPIR" => :lower, # TODO: try to find this one in the xlsx, not sure what this is
    "GPC" => :lower,
    "GCP" => :lower,
    "GLN" => :lower,
    "GTIN" => :lower,
    "SOURCE" => "release",
    "TEL" => "phone",
    "HIER" => "hierarchy",
    "HOTLINE" => :lower,
    "FAX" => :lower,
    "IMG" => "image",
    "INGREDIENTS" => :lower,
    "IRON" => :lower,
    "LANG" => :lower,
    "LAST_CHANGE" => :lower,
    "LINE" => :lower,
    "LINK" => "url",
    "LEVEL" => :lower,
    "OWNER" => :lower,
    "ORIGIN" => :lower,
    "HOTLINE" => :lower,
    "PKG" => "package",
    "PARTY" => :lower,
    "POSTCODE" => :lower,
    "PRODUCT" => :lower,
    "PROTEIN" => :lower,
    "PROVIDER" => :lower,
    "REF" => "reference",
    "RETURN" => :lower,
    "SEARCH" => :lower,
    "SERV" => "serving",
    "SIZE" => :lower,
    "SOD" => "sodium",
    "SOURCE" => :lower,
    "SYNC" => :lower,
    "TOTAL" => :lower,
    "UNIT" => :lower,
    "VITAMIN" => :lower,
    "WEB" => :lower,
  }

  @chunk_size :math.pow(2, 20) |> round

  def from_sql(source, chunk_size \\ @chunk_size) do
    bytes = File.stream!(source, [], chunk_size)
    |> Stream.transform("", fn line, sql ->
      replaced = Enum.reduce(@mappings, line, fn {from, to}, acc ->
        into = if (to == :lower), do: String.downcase from, else: to

        String.replace(acc, from, into)
      end)

      {[replaced], sql <> replaced}
    end)
    |> Enum.join("")

    timestamp = DateTime.utc_now |> DateTime.to_unix |> Integer.to_string
    filename  = "output/#{timestamp}.sql"

    File.write(filename, bytes, [:write, :utf8])
  end

  defp parse_args(args) do
    {options, _, _} = OptionParser.parse(args,
      switches: [source: :string]
    )
    options
  end

  def process([]) do
    {:ok, self()}
  end

  def process(options) do
    IO.puts "Converting POD database to Thing database format ..."

    source = options[:source]
    sql = Pod.Thing.from_sql source

    IO.puts "Done!"

    {:ok, self(), sql}
  end

  def start(_type, args) do
    main args
  end

  def main(args) do
    args |> parse_args |> process
  end
end
