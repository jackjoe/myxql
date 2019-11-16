defmodule MyXQL.GeometryTest do
  use ExUnit.Case, async: true
  use Bitwise

  @default_sql_mode "STRICT_TRANS_TABLES"

  setup do
    conn = connect()
    %{conn: conn}
  end

  test "test name", %{conn: conn} do
    MyXQL.query!(conn, "CREATE TABLE g (name text, p point)")

    statement = "INSERT INTO g (name, p) values (?, ST_GeomFromText(?))"
    MyXQL.query!(conn, statement, ["foo", "POINT(1 1)"])

    statement = "SELECT name FROM g"
    %MyXQL.Result{rows: [rows]} = MyXQL.query!(conn, statement)
    assert rows == ["foo"]

    statement = "SELECT p FROM g"
    %MyXQL.Result{rows: [rows]} = MyXQL.query!(conn, statement)
    assert rows == [%MyXQL.Geometry.Point{coordinates: {1.0, 1.0}, srid: nil}]

    # %MyXQL.Result{rows: [values]} = query!(c, statement)
  end

  #           geoms =
  #             [
  #               "POLYGON((0 0,10 0,10 10,0 10,0 0))",
  #               "POINT(1 1)",
  #               "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0),(0 0,10 0,10 10,0 10,0 0)))",
  #               "MULTIPOLYGON(((102 2,103 2,103 3,102 3,102 2)),((100 0,101 0,101 1,100 1,100 0)))"
  #             ]
  #             |> Enum.map(&poly_roundtrip(c, &1))
  #             |> Enum.map(&IO.inspect/1)
  #
  # defp poly_roundtrip(c, wkt) do
  #   insert =
  #     "INSERT INTO test_types (my_tinyint, my_smallint, my_geom) VALUES (1, 1, ST_GeomFromText(?))"
  #
  #   %MyXQL.Result{last_insert_id: id} = query!(c, insert, [wkt])
  #
  #   select = "SELECT * FROM test_types WHERE id = '#{id}'"
  #   %MyXQL.Result{rows: [values]} = query!(c, select)
  #   IO.inspect(values)
  #   [value] = values
  #   value
  # end

  defp connect() do
    after_connect = fn conn ->
      MyXQL.query!(conn, "SET SESSION sql_mode = '#{@default_sql_mode}'")
    end

    {:ok, conn} = MyXQL.start_link([after_connect: after_connect] ++ TestHelper.opts())
    conn
  end
end
