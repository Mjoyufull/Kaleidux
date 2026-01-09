{
  description: "Kaleidux: High-performance dynamic wallpaper daemon for Wayland and X11";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
    naersk.url = "github:nix-community/naersk/master";
    naersk.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, utils, naersk }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        naersk-lib = pkgs.callPackage naersk { };
        
        runtimeDeps = with pkgs; [
          libwayland-client
          libwayland-egl
          vulkan-loader
          libxkbcommon
          wayland
          xorg.libX11
          xorg.libXcursor
          xorg.libXrandr
          xorg.libXi
          gstreamer
          gst-plugins-base
          gst-plugins-good
          gst-plugins-bad
          gst-plugins-ugly
          gst-libav
        ];

        buildDeps = with pkgs; [
          pkg-config
          cmake
          python3
          llvmPackages.libclang.lib
        ];

      in
      {
        defaultPackage = naersk-lib.buildPackage {
          src = ./.;
          
          nativeBuildInputs = buildDeps;
          buildInputs = runtimeDeps;

          # Environment variables for build
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          
          # Fix for gstreamer-sys
          PKG_CONFIG_PATH = "${pkgs.gstreamer.dev}/lib/pkgconfig:${pkgs.gst-plugins-base.dev}/lib/pkgconfig";

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
      }
    );
}
