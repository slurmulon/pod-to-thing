defmodule Pod.Thing do
  use Application
  alias Experimental.Flow

  # IDEA: could use :lowercase atom so we don't have to do so much error-prone copy pasta
  @mappings %{
    ~r/GS1/im => "gs1",
    ~r/_CD$/m => "_code",
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
    ~r/_DT$/m => "_dt",

    "ADD_PARTY_ID" => :lower,
    "ADDR_02" => "address_line_1",
    "ADDR_03" => "address_line_2",
    "ADDR_04" => "address_line_3",
    "ADDR_POSTCODE" => "address_postcode",
    "ADDR_CITY" => "address_city",
    "BSIN" => :lower,
    "BRAND" => :lower,
    "CONTACT" => :lower,
    "CODE" => :lower,
    "COUNTY" => :lower,
    "COUNTRY" => :lower,
    "DESC" => :lower,
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
    "PROVIDER" => :lower,
    "REF" => "reference",
    "RETURN" => :lower,
    "SEARCH" => :lower,
    "SOURCE" => :lower,
    "SYNC" => :lower,
    "UNIT" => :lower,
    "WEB" => :lower,
  }

  @chunk_size :math.pow(2, 20) |> round

  # WARN: not really working, still trying to determine if it's even possible given the need for order
  def from_sql_async(source, chunk_size \\ @chunk_size) do
   case File.stat source do
     {:ok, %{size: size}} ->
        File.stream!(source, [], chunk_size) # TODO: play with large byte chunks here instead of iterating over each line
        |> Stream.chunk(size / 8) # TODO: determine number of cores
        # |> Stream.map # TODO: map each chunk by its index, so we can sort the chunks at the end
        |> Flow.from_enumerable()
        |> Flow.partition()
        |> Flow.reduce(fn -> %{} end, fn line, sql ->
          {[line], sql <> line}
        end)
        # TODO: Flow.departition
        |> Enum.to_list()
        |> Enum.join()
     {:error, reason} -> IO.puts reason
   end
  end

  def from_sql(source, chunk_size \\ @chunk_size) do
    bytes = File.stream!(source, [], chunk_size)
    |> Stream.transform("", fn line, sql ->
      replaced = Enum.reduce(@mappings, line, fn {from, to}, acc ->
        case to do
          :lower -> String.replace(acc, from, from |> String.downcase)
          _ -> String.replace(acc, from, to)
        end
      end)

      {[replaced], sql <> replaced}
    end)
    |> Enum.join("")

    timestamp = (DateTime.utc_now |> DateTime.to_unix |> Integer.to_string)

    File.write("output/" <> timestamp <> ".sql", bytes, [:write, :utf8])
  end

  defp parse_args(args) do
    {options, _, _} = OptionParser.parse(args,
      switches: [name: :string]
    )
    options
  end

  def start(_type, args) do
    IO.puts "Converting POD database to Thing database format ..."

    # sql = Pod.Thing.from_sql_async("test.sql")
    sql = :timer.tc(fn -> Pod.Thing.from_sql("test.sql") end)# |> IO.inspect

    IO.puts "Done!"

    {:ok, self(), sql}
  end

end
