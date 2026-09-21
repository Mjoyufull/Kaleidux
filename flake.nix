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
        projectSource = self.outPath;
        rustMinVersion = "1.89.0";
        vaDriverDeps = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isx86_64 [
          pkgs.intel-media-driver
        ];
        vaDriverPath = pkgs.lib.makeSearchPath "lib/dri" ([ pkgs.mesa ] ++ vaDriverDeps);
        
        # Runtime libraries are kept entirely inside the Nix ABI closure. On
        # non-NixOS NVIDIA systems, use nixGL rather than adding /usr/lib: a
        # host glibc in LD_LIBRARY_PATH is incompatible with the Nix loader.
        runtimeCoreDeps = with pkgs; [
          wayland
          wayland-protocols
          egl-wayland
          vulkan-loader
          mesa
          libGL
          libglvnd
          libxkbcommon
          gst_all_1.gstreamer
          gst_all_1.gst-plugins-base
          dav1d
        ];
        runtimeBaseDeps = runtimeCoreDeps ++ [ pkgs.libva ] ++ vaDriverDeps;
        x11RuntimeDeps = with pkgs; [
          libx11
          libxcursor
          libxrandr
          libxi
          libxcb
        ];
        appsinkRuntimeDeps = with pkgs; [
          gst_all_1.gst-plugins-good
          gst_all_1.gst-plugins-bad
          gst_all_1.gst-plugins-ugly
          gst_all_1.gst-libav
        ];
        mpvRuntimeDeps = [ pkgs.mpv ];
        ffmpegRuntimeDeps = [ pkgs.ffmpeg ];

        runtimeDeps = runtimeBaseDeps
          ++ x11RuntimeDeps
          ++ appsinkRuntimeDeps
          ++ mpvRuntimeDeps
          ++ ffmpegRuntimeDeps;

        gstPluginPath = pkgs.lib.makeSearchPathOutput "lib" "lib/gstreamer-1.0" (with pkgs.gst_all_1; [
          gstreamer
          gst-plugins-base
          gst-plugins-good
          gst-plugins-bad
          gst-plugins-ugly
          gst-libav
        ]);

        buildDeps =
          assert pkgs.lib.assertMsg
            (pkgs.lib.versionAtLeast pkgs.rustc.version rustMinVersion)
            "Kaleidux requires Rust ${rustMinVersion}+ stable, but nixpkgs provides ${pkgs.rustc.version}";
          with pkgs; [
            pkg-config
            cmake
            python3
            llvmPackages.libclang.lib
            stdenv.cc.libc_dev
            makeWrapper
            binutils
            vulkan-headers
          ];

        mkKaleidux = {
          pname,
          featureArgs ? [ ],
          withAppsink ? true,
          withMpv ? true,
          withFfmpeg ? true,
          withX11 ? true,
        }:
          let
            withVaapi = withFfmpeg || withMpv || withAppsink;
            packageRuntimeDeps = runtimeCoreDeps
              ++ pkgs.lib.optionals withVaapi ([ pkgs.libva ] ++ vaDriverDeps)
              ++ pkgs.lib.optionals withX11 x11RuntimeDeps
              ++ pkgs.lib.optionals withAppsink appsinkRuntimeDeps
              ++ pkgs.lib.optionals withMpv mpvRuntimeDeps
              ++ pkgs.lib.optionals withFfmpeg ffmpegRuntimeDeps;
            packageGstPluginPath = if withAppsink then gstPluginPath else "";
            packagePkgConfigPath =
              "${pkgs.lib.getDev pkgs.gst_all_1.gstreamer}/lib/pkgconfig:${pkgs.lib.getDev pkgs.gst_all_1.gst-plugins-base}/lib/pkgconfig"
              + pkgs.lib.optionalString withVaapi ":${pkgs.lib.getDev pkgs.libva}/lib/pkgconfig"
              + pkgs.lib.optionalString withMpv ":${pkgs.lib.getDev pkgs.mpv}/lib/pkgconfig"
              + pkgs.lib.optionalString withFfmpeg ":${pkgs.lib.getDev pkgs.ffmpeg}/lib/pkgconfig";
            packageVaDriverPrefix = pkgs.lib.optionalString
              (withVaapi && pkgs.stdenv.hostPlatform.isx86_64)
              "--prefix LIBVA_DRIVERS_PATH : \"${vaDriverPath}\" ";
            packageVaWrapperArgs = pkgs.lib.optionalString withVaapi
              "${packageVaDriverPrefix}--suffix LIBVA_DRIVERS_PATH : \"/run/opengl-driver/lib/dri\"";
            packageGstWrapperArgs = pkgs.lib.optionalString withAppsink
              "--prefix GST_PLUGIN_SYSTEM_PATH_1_0 : \"${packageGstPluginPath}\"";
          in naersk-lib.buildPackage {
          inherit pname;
          version = "0.0.1-kneecap";
          # Reuse the already materialized Git-filtered flake tree. Passing the
          # same stable path as root and src avoids a second cleanSource tree.
          root = projectSource;
          src = projectSource;
          cargoBuildOptions = options: options ++ featureArgs;
          cargoTestOptions = options: options ++ featureArgs;
          
          nativeBuildInputs = buildDeps;
          buildInputs = packageRuntimeDeps;

          # Environment variables for build
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          # ffmpeg-sys invokes libclang directly rather than the Nix compiler
          # wrapper, so it does not inherit the wrapper's libc include flags.
          C_INCLUDE_PATH = "${pkgs.stdenv.cc.libc_dev}/include";
          # Cargo's normal release artifact is symbol-stripped. Nix builds one
          # symbol-bearing binary so postInstall can retain a matching debug
          # companion before producing the stripped executable.
          CARGO_PROFILE_RELEASE_STRIP = "none";
          CARGO_PROFILE_RELEASE_DEBUG = "2";
          dontStrip = true;
          
          # Fix for gstreamer-sys
          PKG_CONFIG_PATH = packagePkgConfigPath;

          # Wrap only the daemon, whose graphics/media libraries are loaded at
          # runtime. `--set` prevents an inherited host /usr/lib from mixing a
          # non-NixOS glibc with the Nix ELF interpreter. NixOS exposes vendor
          # drivers through /run/opengl-driver; non-NixOS NVIDIA users should
          # launch this package through nixGL.
          postInstall = ''
            mkdir -p $out/lib/debug
            for binary in kaleidux-daemon kldctl; do
              if [ -x "$out/bin/$binary" ]; then
                ${pkgs.binutils}/bin/objcopy --only-keep-debug \
                  "$out/bin/$binary" "$out/lib/debug/$binary.debug"
                ${pkgs.binutils}/bin/strip --strip-unneeded "$out/bin/$binary"
                ${pkgs.binutils}/bin/objcopy \
                  --add-gnu-debuglink="$out/lib/debug/$binary.debug" \
                  "$out/bin/$binary"
              fi
            done

            wrapProgram $out/bin/kaleidux-daemon \
              --run '
                kld_driver_paths=""
                IFS=: read -r -a kld_library_dirs <<< "''${LD_LIBRARY_PATH-}"
                for kld_library_dir in "''${kld_library_dirs[@]}"; do
                  case "$kld_library_dir" in
                    /nix/store/*|/run/opengl-driver/lib|/run/opengl-driver-32/lib)
                      kld_driver_paths="''${kld_driver_paths:+$kld_driver_paths:}$kld_library_dir"
                      ;;
                  esac
                done
                export LD_LIBRARY_PATH="$kld_driver_paths"
              ' \
              --prefix LD_LIBRARY_PATH : "${pkgs.lib.makeLibraryPath packageRuntimeDeps}:/run/opengl-driver/lib" \
              ${packageVaWrapperArgs} \
              ${packageGstWrapperArgs} \
              --suffix XDG_DATA_DIRS : "/usr/share"
            
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

        kaleidux = mkKaleidux {
          pname = "kaleidux";
        };
        kaleiduxWayland = mkKaleidux {
          pname = "kaleidux-wayland";
          withX11 = false;
          featureArgs = [
            "--no-default-features"
            "--features"
            "kaleidux-daemon/wayland-only"
          ];
        };
        kaleiduxMinimalStatic = mkKaleidux {
          pname = "kaleidux-minimal-static";
          withAppsink = false;
          withMpv = false;
          withFfmpeg = false;
          withX11 = false;
          featureArgs = [
            "--no-default-features"
            "--features"
            "kaleidux-daemon/minimal-static"
          ];
        };
        kaleiduxAppsink = mkKaleidux {
          pname = "kaleidux-appsink";
          withMpv = false;
          withFfmpeg = false;
          withX11 = false;
          featureArgs = [
            "--no-default-features"
            "--features"
            "kaleidux-daemon/display-wayland,kaleidux-daemon/backend-appsink"
          ];
        };
        kaleiduxMpv = mkKaleidux {
          pname = "kaleidux-mpv";
          withAppsink = false;
          withFfmpeg = false;
          withX11 = false;
          featureArgs = [
            "--no-default-features"
            "--features"
            "kaleidux-daemon/display-wayland,kaleidux-daemon/backend-mpv"
          ];
        };
        kaleiduxFfmpeg = mkKaleidux {
          pname = "kaleidux-ffmpeg";
          withAppsink = false;
          withMpv = false;
          withX11 = false;
          featureArgs = [
            "--no-default-features"
            "--features"
            "kaleidux-daemon/display-wayland,kaleidux-daemon/backend-ffmpeg"
          ];
        };

        devShell = with pkgs; mkShell {
          buildInputs = runtimeDeps ++ buildDeps ++ [
            cargo
            rustc
            rustfmt
            clippy
            pre-commit
            rust-analyzer
          ];
          
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath runtimeDeps;
          LIBVA_DRIVERS_PATH = "${vaDriverPath}:/run/opengl-driver/lib/dri:/usr/lib/dri";
          GST_PLUGIN_SYSTEM_PATH_1_0 = gstPluginPath;
        };

      in
      {
        
        packages = {
          default = kaleidux;
          inherit kaleidux;
          wayland-only = kaleiduxWayland;
          minimal-static = kaleiduxMinimalStatic;
          appsink = kaleiduxAppsink;
          mpv = kaleiduxMpv;
          ffmpeg = kaleiduxFfmpeg;
        };

        
        devShells.default = devShell;
      }
    );
}
