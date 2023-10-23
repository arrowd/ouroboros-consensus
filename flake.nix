{
  nixConfig = {
    extra-substituters = [
      "https://cache.iog.io"
    ];
    extra-trusted-public-keys = [
      "hydra.iohk.io:f/Ea+s+dFdN+3Y/G+FDgSq+a5NEWhJGzdjvKNGv0/EQ="
    ];
    allow-import-from-derivation = true;
  };
  inputs = {
    nixpkgs.follows = "haskellNix/nixpkgs-unstable";
    flake-utils.follows = "haskellNix/flake-utils";
    haskellNix = {
      url = "github:input-output-hk/haskell.nix";
      inputs.hackage.follows = "hackageNix";
    };
    hackageNix = {
      url = "github:input-output-hk/hackage.nix";
      flake = false;
    };
    CHaP = {
      url = "github:input-output-hk/cardano-haskell-packages?ref=repo";
      flake = false;
    };
    iohkNix = {
      url = "github:input-output-hk/iohk-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };

    # for cabal-docspec
    cabal-extras = { url = "github:phadej/cabal-extras/cabal-docspec-0.0.0.20230517"; flake = false; };
    gentle-introduction = { url = "github:phadej/gentle-introduction"; flake = false; };
  };
  outputs = inputs:
    let
      supportedSystems = [
        "x86_64-linux"
        "x86_64-darwin"
        #"aarch64-linux"
        "aarch64-darwin"
      ];
    in
    inputs.flake-utils.lib.eachSystem supportedSystems (
      system:
      let
        pkgs = import inputs.nixpkgs {
          inherit system;
          inherit (inputs.haskellNix) config;
          overlays = [
            inputs.iohkNix.overlays.crypto
            inputs.haskellNix.overlay
            inputs.iohkNix.overlays.haskell-nix-crypto
            (import ./nix/tools.nix inputs)
            (import ./nix/haskell.nix inputs)
            (import ./nix/pdfs.nix)
          ];
        };
        hydraJobs = import ./nix/ci.nix pkgs;
      in
      {
        devShells = {
          default = hydraJobs.native.haskell.devShell;
          ghc96 = hydraJobs.native.haskell96.devShell;
          website = pkgs.mkShell {
            packages = [ pkgs.nodejs pkgs.yarn ];
          };
        };
        inherit hydraJobs;
        legacyPackages = pkgs;
      }
    );
}
