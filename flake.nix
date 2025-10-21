{
  description = "DFTracer Utilities";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
  let
    systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
    forAllSystems = f:
      nixpkgs.lib.genAttrs systems (system:
        f system (import nixpkgs { inherit system; }));
  in
  {
    devShells = forAllSystems (system: pkgs:
      let
        gcc = pkgs.gcc14;
      in {
        default = pkgs.mkShell {
          packages = [ gcc ] ++ (with pkgs; [
            cmake
            ninja
            pkg-config
            pigz
            lcov
            openmpi
            cmake-format
            doxygen
            graphviz
            # valgrind-light
            # sqlite
            # zlib
            # spdlog
            gnutar
            python311
            python312
            (python310.withPackages (p: [
              p.cython
              p.setuptools
              p.wheel
              p.venvShellHook
            ]))
          ]);

          CC = "gcc";
          CXX = "g++";
          shellHook = ''
            export CC=gcc
            export CXX=g++
            unset SOURCE_DATE_EPOCH
          '';
        };
      });
  };
}
