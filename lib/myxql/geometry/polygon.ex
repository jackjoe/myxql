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
