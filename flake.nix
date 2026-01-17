{
  description = "Kaleidux: High-performance dynamic wallpaper daemon for Wayland and X11";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
    naersk.url = "github:nix-community/naersk/master";
    naersk.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, utils, naersk }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        naersk-lib = pkgs.callPackage naersk { };
        
        runtimeDeps = with pkgs; [
          wayland
          egl-wayland
          vulkan-loader
          libxkbcommon
          xorg.libX11
          xorg.libXcursor
          xorg.libXrandr
          xorg.libXi
          gst_all_1.gstreamer
          gst_all_1.gst-plugins-base
          gst_all_1.gst-plugins-good
          gst_all_1.gst-plugins-bad
          gst_all_1.gst-plugins-ugly
          gst_all_1.gst-libav
        ];

        buildDeps = with pkgs; [
          pkg-config
          cmake
          python3
          llvmPackages.libclang.lib
          makeWrapper
        ];

        kaleidux = naersk-lib.buildPackage {
          src = ./.;
          
          nativeBuildInputs = buildDeps;
          buildInputs = runtimeDeps;

          # Environment variables for build
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          
          # Fix for gstreamer-sys
          PKG_CONFIG_PATH = "${pkgs.gst_all_1.gstreamer.dev}/lib/pkgconfig:${pkgs.gst_all_1.gst-plugins-base.dev}/lib/pkgconfig";

          # Post-install hook to wrap binaries with runtime path
          postInstall = ''
            wrapProgram $out/bin/kaleidux-daemon \
              --prefix LD_LIBRARY_PATH : "${pkgs.lib.makeLibraryPath runtimeDeps}"
            wrapProgram $out/bin/kldctl \
              --prefix LD_LIBRARY_PATH : "${pkgs.lib.makeLibraryPath runtimeDeps}"
          '';

          meta = with pkgs.lib; {
            description = "High-performance dynamic wallpaper daemon";
            homepage = "https://github.com/Mjoyufull/Kaleidux";
            license = licenses.agpl3Only;
            platforms = platforms.linux;
          };
        };

        devShell = with pkgs; mkShell {
          buildInputs = runtimeDeps ++ buildDeps ++ [
            cargo
            rustc
            rustfmt
            pre-commit
            rust-analyzer
          ];
          
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath runtimeDeps;
        };

      in
      {
        
        packages = {
          default = kaleidux;
          kaleidux = kaleidux;
        };

        
        defaultPackage = kaleidux;

        
        devShells.default = devShell;

        
        devShell = devShell;
      }
    );
}
