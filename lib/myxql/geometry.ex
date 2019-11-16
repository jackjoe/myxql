defmodule MyXQL.Geometry.Point do
  @moduledoc """
  Define the Point struct
  """

  @type t :: %MyXQL.Geometry.Point{coordinates: {number, number}, srid: non_neg_integer | nil}
  defstruct coordinates: {0, 0}, srid: nil
end

defmodule MyXQL.Geometry.Polygon do
  @moduledoc """
  Define the Polygon struct
  """

  @type t :: %MyXQL.Geometry.Polygon{
          coordinates: [[{number, number}]],
          srid: non_neg_integer | nil
        }
  defstruct coordinates: [], srid: nil
end

defmodule MyXQL.Geometry.MultiPolygon do
  @moduledoc """
  Define the MultiPolygon struct
  """

  @type t :: %MyXQL.Geometry.MultiPolygon{
          coordinates: [[[{number, number}]]],
          srid: non_neg_integer | nil
        }
  defstruct coordinates: [], srid: nil
end
