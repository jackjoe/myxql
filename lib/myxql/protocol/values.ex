defmodule MyXQL.Protocol.Values do
  @moduledoc false

  use Bitwise
  import MyXQL.Protocol.Types
  import MyXQL.Protocol.Records, only: [column_def: 1]

  # Text & Binary row value encoding/decoding
  #
  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
  # https://dev.mysql.com/doc/refman/8.0/en/data-types.html

  # TIMESTAMP vs DATETIME
  #
  # https://dev.mysql.com/doc/refman/8.0/en/datetime.html
  # MySQL converts TIMESTAMP values from the current time zone to UTC for
  # storage, and back from UTC to the current time zone for retrieval. (This
  # does not occur for other types such as DATETIME.)
  #
  # Comparing to Postgres we have:
  # MySQL TIMESTAMP is equal to Postgres TIMESTAMP WITH TIME ZONE
  # MySQL DATETIME  is equal to Postgres TIMESTAMP [WITHOUT TIME ZONE]

  types = [
    mysql_type_tiny: 0x01,
    mysql_type_short: 0x02,
    mysql_type_long: 0x03,
    mysql_type_float: 0x04,
    mysql_type_double: 0x05,
    # https://dev.mysql.com/doc/internals/en/null-bitmap.html
    mysql_type_null: 0x06,
    mysql_type_timestamp: 0x07,
    mysql_type_longlong: 0x08,
    mysql_type_int24: 0x09,
    mysql_type_date: 0x0A,
    mysql_type_time: 0x0B,
    mysql_type_datetime: 0x0C,
    mysql_type_year: 0x0D,
    mysql_type_varchar: 0x0F,
    mysql_type_bit: 0x10,
    mysql_type_json: 0xF5,
    mysql_type_newdecimal: 0xF6,
    mysql_type_enum: 0xF7,
    mysql_type_set: 0xF8,
    mysql_type_tiny_blob: 0xF9,
    mysql_type_medium_blob: 0xFA,
    mysql_type_long_blob: 0xFB,
    mysql_type_blob: 0xFC,
    mysql_type_var_string: 0xFD,
    mysql_type_string: 0xFE,
    mysql_type_geometry: 0xFF
  ]

  for {atom, code} <- types do
    def type_code_to_atom(unquote(code)), do: unquote(atom)
    def type_atom_to_code(unquote(atom)), do: unquote(code)
  end

  defp column_def_to_type(column_def(type: :mysql_type_tiny, unsigned?: true)), do: :uint1
  defp column_def_to_type(column_def(type: :mysql_type_tiny, unsigned?: false)), do: :int1
  defp column_def_to_type(column_def(type: :mysql_type_short, unsigned?: true)), do: :uint2
  defp column_def_to_type(column_def(type: :mysql_type_short, unsigned?: false)), do: :int2
  defp column_def_to_type(column_def(type: :mysql_type_long, unsigned?: true)), do: :uint4
  defp column_def_to_type(column_def(type: :mysql_type_long, unsigned?: false)), do: :int4
  defp column_def_to_type(column_def(type: :mysql_type_int24, unsigned?: true)), do: :uint4
  defp column_def_to_type(column_def(type: :mysql_type_int24, unsigned?: false)), do: :int4
  defp column_def_to_type(column_def(type: :mysql_type_longlong, unsigned?: true)), do: :uint8
  defp column_def_to_type(column_def(type: :mysql_type_longlong, unsigned?: false)), do: :int8
  defp column_def_to_type(column_def(type: :mysql_type_year)), do: :uint2
  defp column_def_to_type(column_def(type: :mysql_type_float)), do: :float
  defp column_def_to_type(column_def(type: :mysql_type_double)), do: :double
  defp column_def_to_type(column_def(type: :mysql_type_timestamp)), do: :datetime
  defp column_def_to_type(column_def(type: :mysql_type_date)), do: :date
  defp column_def_to_type(column_def(type: :mysql_type_time)), do: :time
  defp column_def_to_type(column_def(type: :mysql_type_datetime)), do: :naive_datetime
  defp column_def_to_type(column_def(type: :mysql_type_newdecimal)), do: :decimal
  defp column_def_to_type(column_def(type: :mysql_type_json)), do: :json
  defp column_def_to_type(column_def(type: :mysql_type_blob)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_tiny_blob)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_medium_blob)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_long_blob)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_var_string)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_string)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_bit, length: length)), do: {:bit, length}
  defp column_def_to_type(column_def(type: :mysql_type_null)), do: :null

  # geometry
  defp column_def_to_type(column_def(type: :mysql_type_geometry)), do: :geometry

  # Text values

  def decode_text_row(values, column_defs) do
    types = Enum.map(column_defs, &column_def_to_type/1)
    decode_text_row(values, types, [])
  end

  # null value
  defp decode_text_row(<<0xFB, rest::binary>>, [_type | tail], acc) do
    decode_text_row(rest, tail, [nil | acc])
  end

  defp decode_text_row(<<values::binary>>, [type | tail], acc) do
    {string, rest} = take_string_lenenc(values)
    value = decode_text_value(string, type)
    decode_text_row(rest, tail, [value | acc])
  end

  defp decode_text_row("", _column_type, acc) do
    Enum.reverse(acc)
  end

  def decode_text_value(value, type)
      when type in [
             :uint1,
             :uint2,
             :uint4,
             :uint8,
             :int1,
             :int2,
             :int4,
             :int8
           ] do
    String.to_integer(value)
  end

  def decode_text_value(value, type) when type in [:float, :double] do
    String.to_float(value)
  end

  # Note: MySQL implements `NUMERIC` as `DECIMAL`s
  def decode_text_value(value, :decimal) do
    Decimal.new(value)
  end

  def decode_text_value("0000-00-00", :date) do
    :zero_date
  end

  def decode_text_value(value, :date) do
    Date.from_iso8601!(value)
  end

  def decode_text_value(value, :time) do
    Time.from_iso8601!(value)
  end

  def decode_text_value("0000-00-00 00:00:00", :naive_datetime) do
    :zero_datetime
  end

  def decode_text_value(value, :naive_datetime) do
    NaiveDateTime.from_iso8601!(value)
  end

  def decode_text_value("0000-00-00 00:00:00", :datetime) do
    :zero_datetime
  end

  def decode_text_value(value, :datetime) do
    value
    |> NaiveDateTime.from_iso8601!()
    |> DateTime.from_naive!("Etc/UTC")
  end

  def decode_text_value(value, :binary) do
    value
  end

  def decode_text_value(value, :json) do
    json_library().decode!(value)
  end

  def decode_text_value(value, {:bit, size}) do
    decode_bit(value, size)
  end

  # Binary values

  def encode_binary_value(value)
      when is_integer(value) and value >= -1 <<< 63 and value < 1 <<< 64 do
    {:mysql_type_longlong, <<value::int8>>}
  end

  def encode_binary_value(value) when is_float(value) do
    {:mysql_type_double, <<value::64-little-signed-float>>}
  end

  def encode_binary_value(%Decimal{} = value) do
    string = Decimal.to_string(value, :normal)
    {:mysql_type_newdecimal, <<byte_size(string), string::binary>>}
  end

  def encode_binary_value(%Date{year: year, month: month, day: day}) do
    {:mysql_type_date, <<4, year::uint2, month::uint1, day::uint1>>}
  end

  def encode_binary_value(%Time{} = time), do: encode_binary_time(time)

  def encode_binary_value(%NaiveDateTime{} = datetime), do: encode_binary_datetime(datetime)

  def encode_binary_value(%DateTime{} = datetime), do: encode_binary_datetime(datetime)

  def encode_binary_value(binary) when is_binary(binary) do
    {:mysql_type_var_string, encode_string_lenenc(binary)}
  end

  def encode_binary_value(bitstring) when is_bitstring(bitstring) do
    size = bit_size(bitstring)
    pad = 8 - rem(size, 8)
    bitstring = <<0::size(pad), bitstring::bitstring>>
    {:mysql_type_bit, encode_string_lenenc(bitstring)}
  end

  def encode_binary_value(true) do
    {:mysql_type_tiny, <<1>>}
  end

  def encode_binary_value(false) do
    {:mysql_type_tiny, <<0>>}
  end

  def encode_binary_value(term) when is_list(term) or is_map(term) do
    string = json_library().encode!(term)
    {:mysql_type_var_string, encode_string_lenenc(string)}
  end

  def encode_binary_value(other) do
    raise ArgumentError, "query has invalid parameter #{inspect(other)}"
  end

  ## Time/DateTime

  # MySQL supports negative time and days, we don't.
  # See: https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_TIME

  defp encode_binary_time(%Time{hour: 0, minute: 0, second: 0, microsecond: {0, 0}}) do
    {:mysql_type_time, <<0>>}
  end

  defp encode_binary_time(%Time{hour: hour, minute: minute, second: second, microsecond: {0, 0}}) do
    {:mysql_type_time, <<8, 0::uint1, 0::uint4, hour::uint1, minute::uint1, second::uint1>>}
  end

  defp encode_binary_time(%Time{
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {microsecond, _}
       }) do
    {:mysql_type_time,
     <<12, 0::uint1, 0::uint4, hour::uint1, minute::uint1, second::uint1, microsecond::uint4>>}
  end

  defp encode_binary_datetime(%NaiveDateTime{
         year: year,
         month: month,
         day: day,
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {0, 0}
       }) do
    {:mysql_type_datetime,
     <<7, year::uint2, month::uint1, day::uint1, hour::uint1, minute::uint1, second::uint1>>}
  end

  defp encode_binary_datetime(%NaiveDateTime{
         year: year,
         month: month,
         day: day,
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {microsecond, _}
       }) do
    {:mysql_type_datetime,
     <<11, year::uint2, month::uint1, day::uint1, hour::uint1, minute::uint1, second::uint1,
       microsecond::uint4>>}
  end

  defp encode_binary_datetime(%DateTime{
         year: year,
         month: month,
         day: day,
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {microsecond, _},
         time_zone: "Etc/UTC"
       }) do
    {:mysql_type_datetime,
     <<11, year::uint2, month::uint1, day::uint1, hour::uint1, minute::uint1, second::uint1,
       microsecond::uint4>>}
  end

  defp encode_binary_datetime(%DateTime{} = datetime) do
    raise ArgumentError, "#{inspect(datetime)} is not in UTC"
  end

  def decode_binary_row(<<payload::bits>>, column_defs) do
    size = div(length(column_defs) + 7 + 2, 8)
    <<0x00, null_bitmap::uint(size), values::bits>> = payload
    null_bitmap = null_bitmap >>> 2
    types = Enum.map(column_defs, &column_def_to_type/1)
    decode_binary_row(values, null_bitmap, types, [])
  end

  defp decode_binary_row(<<rest::bits>>, null_bitmap, [_type | t], acc)
       when (null_bitmap &&& 1) == 1 do
    decode_binary_row(rest, null_bitmap >>> 1, t, [nil | acc])
  end

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:binary | t], acc),
    do: decode_string_lenenc(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:int1 | t], acc),
    do: decode_int1(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:uint1 | t], acc),
    do: decode_uint1(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:int2 | t], acc),
    do: decode_int2(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:uint2 | t], acc),
    do: decode_uint2(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:int4 | t], acc),
    do: decode_int4(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:uint4 | t], acc),
    do: decode_uint4(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:int8 | t], acc),
    do: decode_int8(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:uint8 | t], acc),
    do: decode_uint8(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:float | t], acc),
    do: decode_float(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:double | t], acc),
    do: decode_double(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:decimal | t], acc),
    do: decode_decimal(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:json | t], acc),
    do: decode_json(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:date | t], acc),
    do: decode_date(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:time | t], acc),
    do: decode_time(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:naive_datetime | t], acc),
    do: decode_datetime(r, null_bitmap, t, acc, :naive_datetime)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:datetime | t], acc),
    do: decode_datetime(r, null_bitmap, t, acc, :datetime)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:geometry | t], acc) do
    decode_geometry_head(r) |> decode_geometry(null_bitmap, t, acc)
  end

  @doc """
  0xFC = 252
  """
  def decode_geometry_head(<<0xFC, _::uint2, r::bits>>), do: r
  def decode_geometry_head(<<0xFD, _::uint3, r::bits>>), do: r
  def decode_geometry_head(<<0xFE, _::uint8, r::bits>>), do: r
  def decode_geometry_head(<<_lt251::uint1, r::bits>>), do: r

  # https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
  # defp decode_json(<<n::uint1, v::string(n), r::bits>>, null_bitmap, t, acc) when n < 251,

  # Point
  defp decode_geometry(
         <<_srid::uint4, 1::uint1, 1::uint4, x::little-float-64, y::little-float-64, r::bits>>,
         null_bitmap,
         t,
         acc,
         _
       ) do
    v = %Geo.Point{coordinates: {x, y}, properties: %{}, srid: nil}
    decode_binary_row(r, null_bitmap, t, [v | acc])
  end

  # Polygon
  defp decode_geometry(
         <<srid::uint4, 1::uint1, 3::uint4, num_rings::uint4, rest::bits>>,
         null_bitmap,
         t,
         acc
       ) do
    IO.puts("==> step 4, decode_geometry")
    decode_rings(rest, num_rings, {srid, null_bitmap, t, acc})
  end

  # MultiPolygon
  # <<len::uint1, srid::uint4, 1::uint1, 6::uint4, num_rings::uint4, rest::bits>>,
  defp decode_geometry(
         <<srid::uint4, 1::uint1, 6::uint4, r::bits>>,
         null_bitmap,
         t,
         acc
       ) do
    decode_multipolygon(r, {srid, null_bitmap, t, acc})
  end

  ### GEOMETRY HELPERS

  # myxql
  # defp decode_binary_row(<<r::bits>>, null_bitmap, [:double | t], acc),
  # mariaex
  # defp decode_bin_rows(<<rest::bits>>, [_ | fields], nullint, acc, datetime, json_library)

  # Geometry helpers, inspired (aka copied from) Mariaex. But, dissected and we understand what they do!

  @doc """
  Helps to decode a Polygon, which consists of rings that themselves consist of points
  """

  defp decode_rings(<<rings_and_rows::bits>>, num_rings, state) do
    # IO.puts("==> step 5, decode_rings entry point")
    decode_rings(rings_and_rows, num_rings, state, [])
  end

  defp decode_rings(
         <<r::bits>>,
         0,
         {srid, null_bitmap, t, acc},
         rings
       ) do
    decode_binary_row(
      r,
      null_bitmap,
      t,
      [rings | acc]
    )
  end

  defp decode_rings(
         <<num_points::32-little, points::binary-size(num_points)-unit(128), r::bits>>,
         num_rings,
         state,
         nested,
         rings
       ) do
    points = decode_points(points)
    decode_rings(r, num_rings - 1, state, [points | rings], nested)
  end

  defp decode_points(points_binary, points \\ [])

  defp decode_points(<<x::little-float-64, y::little-float-64, r::bits>>, points) do
    decode_points(r, [{x, y} | points])
  end

  defp decode_points(<<>>, points), do: Enum.reverse(points)

  # MultiPolygon decoding

  defp decode_multipolygon(
         <<num_polygons::uint4, r::bits>> = data,
         state
       ) do
    IO.puts(">> decode multip start, polygons in MP: #{num_polygons}")
    IO.inspect(data)
    IO.inspect(r)
    decode_multipolygon(r, num_polygons, state, [])
  end

  defp decode_multipolygon(<<r::bits>>, 0, {srid, null_bitmap, t, acc}, polygons) do
    IO.puts("Decode MultiPolygon")
    IO.puts("==> step 1, polygon count: #{0} polygons")

    IO.puts("==> step 4, no polygons left")
    g = decode_geometry(r, null_bitmap, t, acc, true)
    IO.puts(">>> THE G")
    IO.inspect(g)

    # decode_binary_row(
    #   r,
    #   null_bitmap,
    #   t,
    #   [%Geo.MultiPolygon{coordinates: Enum.reverse(polygons), srid: srid} | acc]
    # )
  end

  defp decode_multipolygon(
         <<1::uint1, 3::uint4, num_rings::uint4, rest::bits>> = d,
         num_polygons,
         state,
         polygons
       ) do
    IO.puts("==> step 2, #{num_polygons} Polygons left")
    IO.puts("==> decode Polygon in MultiPolygon with #{num_rings} rings")
    IO.puts("==> the data blob")
    IO.inspect(d)
    polygon = decode_multipolygon_rings(rest, num_rings, state, [])
    polygon = List.flatten(polygon)
    IO.puts("==> step 3, polygon")
    IO.inspect(polygon)
    decode_multipolygon(rest, num_polygons - 1, state, [polygon | polygons])
  end

  # defp decode_multipolygon(
  #        <<rest::bits>>,
  #        num_polygons,
  #        state,
  #        polygons
  #      ) do
  #   IO.puts("==> step 6, #{num_polygons} polygons left")
  #   IO.puts("==> dead end")
  #   IO.inspect(rest)
  #   polygon = decode_multipolygon_rings(rest, num_rings, state, [])
  #   polygon = List.flatten(polygon)
  #   IO.puts("==> step 5, polygon")
  #   # IO.inspect(polygon)
  #   # decode_multipolygon(rest, num_polygons - 1, state, [polygon | polygons])
  # end

  defp decode_multipolygon_rings(
         <<rest::bits>>,
         0,
         {srid, null_bitmap, t, acc},
         rings
       ) do
    # decode_binary_row(
    #   rest,
    #   null_bitmap,
    #   t,
    [Enum.reverse(rings) | acc]
    # )
  end

  defp decode_multipolygon_rings(
         <<num_points::32-little, points::binary-size(num_points)-unit(128), rest::bits>>,
         num_rings,
         state,
         rings
       ) do
    points = decode_points(points)
    decode_multipolygon_rings(rest, num_rings - 1, state, [points | rings])
  end

  #   defp decode_polygons(<<polygons::bits>>, 0, state, polygons) do
  # polygons = decode_rings()
  #   end
  #
  #   defp decode_polygons(<<polygons::bits>>, 0, state, polygons) do
  #
  #   end

  # end geometry helpers

  defp decode_binary_row(<<r::bits>>, null_bitmap, [{:bit, size} | t], acc),
    do: decode_bit(r, size, null_bitmap, t, acc)

  defp decode_binary_row(<<>>, _null_bitmap, [], acc) do
    Enum.reverse(acc)
  end

  defp decode_int1(<<v::int1, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint1(<<v::uint1, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_int2(<<v::int2, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint2(<<v::uint2, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_int4(<<v::int4, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint4(<<v::uint4, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_int8(<<v::int8, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint8(<<v::uint8, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_float(<<v::32-signed-little-float, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_double(<<v::64-signed-little-float, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  # in theory it's supposed to be a `string_lenenc` field. However since MySQL decimals
  # maximum precision is 65 digits, the size of the string will always fir on one byte.
  defp decode_decimal(<<n::uint1, string::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [Decimal.new(string) | acc])

  defp decode_date(<<4, year::uint2, month::uint1, day::uint1, r::bits>>, null_bitmap, t, acc) do
    v = %Date{year: year, month: month, day: day}
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_date(<<0, r::bits>>, null_bitmap, t, acc) do
    v = :zero_date
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_time(
         <<8, 0, 0::uint4, hour::uint1, minute::uint1, second::uint1, r::bits>>,
         null_bitmap,
         t,
         acc
       ) do
    v = %Time{hour: hour, minute: minute, second: second}
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_time(
         <<12, 0, 0::uint4, hour::uint1, minute::uint1, second::uint1, microsecond::uint4,
           r::bits>>,
         null_bitmap,
         t,
         acc
       ) do
    v = %Time{hour: hour, minute: minute, second: second, microsecond: {microsecond, 6}}
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_time(<<0, r::bits>>, null_bitmap, t, acc) do
    v = ~T[00:00:00]
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_datetime(
         <<4, year::uint2, month::uint1, day::uint1, r::bits>>,
         null_bitmap,
         t,
         acc,
         type
       ) do
    v = new_datetime(type, year, month, day, 0, 0, 0, {0, 0})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_datetime(
         <<7, year::uint2, month::uint1, day::uint1, hour::uint1, minute::uint1, second::uint1,
           r::bits>>,
         null_bitmap,
         t,
         acc,
         type
       ) do
    v = new_datetime(type, year, month, day, hour, minute, second, {0, 0})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_datetime(
         <<11, year::uint2, month::uint1, day::uint1, hour::uint1, minute::uint1, second::uint1,
           microsecond::uint4, r::bits>>,
         null_bitmap,
         t,
         acc,
         type
       ) do
    v = new_datetime(type, year, month, day, hour, minute, second, {microsecond, 6})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_datetime(
         <<0, r::bits>>,
         null_bitmap,
         t,
         acc,
         _type
       ) do
    v = :zero_datetime
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp new_datetime(:datetime, year, month, day, hour, minute, second, microsecond) do
    %DateTime{
      year: year,
      month: month,
      day: day,
      hour: hour,
      minute: minute,
      second: second,
      microsecond: microsecond,
      std_offset: 0,
      time_zone: "Etc/UTC",
      utc_offset: 0,
      zone_abbr: "UTC"
    }
  end

  defp new_datetime(:naive_datetime, year, month, day, hour, minute, second, microsecond) do
    %NaiveDateTime{
      year: year,
      month: month,
      day: day,
      hour: hour,
      minute: minute,
      second: second,
      microsecond: microsecond
    }
  end

  defp decode_string_lenenc(<<n::uint1, v::string(n), r::bits>>, null_bitmap, t, acc)
       when n < 251,
       do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_string_lenenc(<<0xFC, n::uint2, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_string_lenenc(<<0xFD, n::uint3, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_string_lenenc(<<0xFE, n::uint8, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_json(<<n::uint1, v::string(n), r::bits>>, null_bitmap, t, acc) when n < 251,
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(<<0xFC, n::uint2, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(<<0xFD, n::uint3, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(<<0xFE, n::uint8, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(string), do: json_library().decode!(string)

  defp json_library() do
    Application.get_env(:myxql, :json_library, Jason)
  end

  defp decode_bit(<<n::uint1, v::string(n), r::bits>>, size, null_bitmap, t, acc) when n < 251,
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_bit(v, size) | acc])

  defp decode_bit(<<0xFC, n::uint2, v::string(n), r::bits>>, size, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_bit(v, size) | acc])

  defp decode_bit(<<0xFD, n::uint3, v::string(n), r::bits>>, size, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_bit(v, size) | acc])

  defp decode_bit(<<0xFE, n::uint8, v::string(n), r::bits>>, size, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_bit(v, size) | acc])

  defp decode_bit(binary, size) do
    pad = 8 - rem(size, 8)
    <<0::size(pad), bitstring::size(size)-bits>> = binary
    bitstring
  end
end
