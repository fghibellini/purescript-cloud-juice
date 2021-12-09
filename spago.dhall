{-
Welcome to a Spago project!
You can edit this file as you like.
-}
{ name =
    "cloud-juice"
, dependencies =
    [ "affjax"
    , "aff-retry"
    , "avar"
    , "console"
    , "debug"
    , "effect"
    , "foreign"
    , "foreign-generic"
    , "formatters"
    , "flow-id"
    , "node-fs"
    , "node-http"
    , "node-process"
    , "node-streams"
    , "nullable"
    , "prelude"
    , "psci-support"
    , "quickcheck"
    , "simple-json"
    , "spec-discovery"
    , "spec-quickcheck"
    , "transformers"
    , "uuid"
    , "variant"
    , "express"
    ]
, packages =
    ./packages.dhall
, sources =
    [ "src/**/*.purs", "test/**/*.purs" ]
}
