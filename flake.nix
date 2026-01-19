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
        
        # Runtime deps for linking - vulkan-loader needed for build but NOT in runtime path
        # On non-NixOS, system's vulkan-loader must be used to find system ICD files
        runtimeDeps = with pkgs; [
          wayland
          wayland-protocols
          egl-wayland
          vulkan-loader  # Needed for linking, but excluded from runtime LD_LIBRARY_PATH
          mesa
          libGL
          libglvnd
          libxkbcommon
          xorg.libX11
          xorg.libXcursor
          xorg.libXrandr
          xorg.libXi
          xorg.libxcb
          gst_all_1.gstreamer
          gst_all_1.gst-plugins-base
          gst_all_1.gst-plugins-good
          gst_all_1.gst-plugins-bad
          gst_all_1.gst-plugins-ugly
          gst_all_1.gst-libav
        ];
        
        # Runtime deps for wrapping - excludes vulkan-loader so system loader is used
        runtimeWrapDeps = with pkgs; [
          wayland
          wayland-protocols
          egl-wayland
          vulkan-loader
          mesa
          libGL
          libglvnd
          libxkbcommon
          xorg.libX11
          xorg.libXcursor
          xorg.libXrandr
          xorg.libXi
          xorg.libxcb
          gst_all_1.gstreamer
          gst_all_1.gst-plugins-base
          gst_all_1.gst-plugins-good
          gst_all_1.gst-plugins-bad
          gst_all_1.gst-plugins-ugly
          gst_all_1.gst-libav
        ];

        gstPluginPath = pkgs.lib.makeSearchPathOutput "lib" "lib/gstreamer-1.0" (with pkgs.gst_all_1; [
          gstreamer
          gst-plugins-base
          gst-plugins-good
          gst-plugins-bad
          gst-plugins-ugly
          gst-libav
        ]);

        buildDeps = with pkgs; [
          pkg-config
          cmake
          python3
          llvmPackages.libclang.lib
          makeWrapper
          vulkan-headers
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
          # Use --suffix to append Nix libraries AFTER system default paths
          # This allows system graphics drivers (NVIDIA, etc.) to be found first
          # System libraries in /usr/lib, /lib are checked before Nix store paths
          # Exclude vulkan-loader from runtime path - system's loader must be used
          # to find system ICD files (NVIDIA drivers, etc.)
          postInstall = ''
            wrapProgram $out/bin/kaleidux-daemon \
              --suffix LD_LIBRARY_PATH : "${pkgs.lib.makeLibraryPath runtimeWrapDeps}" \
              --suffix LD_LIBRARY_PATH : "/run/opengl-driver/lib:/run/opengl-driver-32/lib:/usr/lib" \
              --prefix GST_PLUGIN_SYSTEM_PATH_1_0 : "${gstPluginPath}" \
              --suffix XDG_DATA_DIRS : "/usr/share"
            wrapProgram $out/bin/kldctl \
              --suffix LD_LIBRARY_PATH : "${pkgs.lib.makeLibraryPath runtimeWrapDeps}" \
              --suffix LD_LIBRARY_PATH : "/run/opengl-driver/lib:/run/opengl-driver-32/lib:/usr/lib"
            
            mkdir -p $out/share/man/man1
            cp man/kaleidux-daemon.1 $out/share/man/man1/
            cp man/kldctl.1 $out/share/man/man1/
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
