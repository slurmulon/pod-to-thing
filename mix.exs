defmodule Pod.Thing.Mixfile do
  use Mix.Project

  def project do
    [app: :pod_thing,
     version: "0.1.0",
     elixir: "~> 1.3",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     escript: [main_module: Pod.Thing],
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [
      mod: {Pod.Thing, []},
      applications: [:logger, :gen_stage]
    ]
  end

  defp deps do
    [{:gen_stage, "~> 0.4"}]
  end
end
