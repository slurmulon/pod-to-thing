# pod-to-thing

> :cd: Converts the P.O.D. public database into Thing's API format

---

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add `pod_thing` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:pod_thing, "~> 0.1.0"}]
    end
    ```

  2. Ensure `pod_thing` is started before your application:

    ```elixir
    def application do
      [applications: [:pod_thing]]
    end
    ```
## Setup

If you would like to run the app as a CLI, first run the following:

`mix escript.build`

You can then use the binary as follows:

`./pod_thing --source-pod.sql`
