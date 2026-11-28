//! On-demand MPRIS access from the control socket, outside the display loop.

use kaleidux_common::{MediaPlayerRequest, Response};

#[cfg(not(feature = "desktop-player"))]
pub(crate) async fn handle(_: MediaPlayerRequest) -> Response {
    Response::Error("This build has no desktop player support".into())
}

#[cfg(feature = "desktop-player")]
pub(crate) use mpris::handle;

#[cfg(feature = "desktop-player")]
mod mpris {
    use super::{MediaPlayerRequest, Response};
    use anyhow::{Context, Result};
    use gio::prelude::CancellableExt;
    use gio::{Cancellable, DBusCallFlags, DBusConnection};
    use glib::variant::{FromVariant, ObjectPath, StaticVariantType, ToVariant};
    use glib::{Variant, VariantTy};
    use kaleidux_common::{MediaPlayerAction, MediaPlayerStatus, PlaybackState};
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    const NAME_PREFIX: &str = "org.mpris.MediaPlayer2.";
    const OBJECT: &str = "/org/mpris/MediaPlayer2";
    const PLAYER_INTERFACE: &str = "org.mpris.MediaPlayer2.Player";
    const PROPERTIES_INTERFACE: &str = "org.freedesktop.DBus.Properties";
    const CALL_TIMEOUT_MS: i32 = 350;
    const REQUEST_TIMEOUT: Duration = Duration::from_millis(1600);
    static REQUESTS: Semaphore = Semaphore::const_new(4);
    static CONNECTION: Mutex<Option<DBusConnection>> = Mutex::new(None);

    type Properties = HashMap<String, Variant>;

    pub(crate) async fn handle(request: MediaPlayerRequest) -> Response {
        let Ok(permit) = REQUESTS.try_acquire() else {
            return Response::Error("Desktop player requests are busy".into());
        };
        let cancel = Cancellable::new();
        let worker_cancel = cancel.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            execute(request, &worker_cancel)
        });
        match tokio::time::timeout(REQUEST_TIMEOUT, worker).await {
            Ok(Ok(Ok(response))) => response,
            Ok(Ok(Err(error))) => Response::Error(format!("{error:#}")),
            Ok(Err(_)) => Response::Error("Desktop player worker stopped".into()),
            Err(_) => {
                cancel.cancel();
                Response::Error("Desktop player request timed out".into())
            }
        }
    }

    fn session_bus(cancel: &Cancellable) -> Result<DBusConnection> {
        if let Some(connection) = CONNECTION
            .lock()
            .unwrap()
            .as_ref()
            .filter(|connection| !connection.is_closed())
        {
            return Ok(connection.clone());
        }
        let address = gio::dbus_address_get_for_bus_sync(gio::BusType::Session, Some(cancel))?;
        let connection = DBusConnection::for_address_sync(
            &address,
            gio::DBusConnectionFlags::AUTHENTICATION_CLIENT
                | gio::DBusConnectionFlags::MESSAGE_BUS_CONNECTION,
            None,
            Some(cancel),
        )?;
        *CONNECTION.lock().unwrap() = Some(connection.clone());
        Ok(connection)
    }

    fn read_properties(
        connection: &DBusConnection,
        name: &str,
        cancel: &Cancellable,
    ) -> Result<(PlaybackState, Properties)> {
        let answer = connection.call_sync(
            Some(name),
            OBJECT,
            PROPERTIES_INTERFACE,
            "GetAll",
            Some(&(PLAYER_INTERFACE,).to_variant()),
            Some(VariantTy::new("(a{sv})").expect("valid properties signature")),
            DBusCallFlags::NO_AUTO_START,
            CALL_TIMEOUT_MS,
            Some(cancel),
        )?;
        let (properties,) = answer
            .get::<(Properties,)>()
            .context("Invalid desktop player properties")?;
        let playback = match read::<String>(&properties, "PlaybackStatus").as_deref() {
            Some("Playing") => PlaybackState::Playing,
            Some("Paused") => PlaybackState::Paused,
            Some("Stopped") => PlaybackState::Stopped,
            _ => anyhow::bail!("Invalid desktop player playback state"),
        };
        Ok((playback, properties))
    }

    fn execute(request: MediaPlayerRequest, cancel: &Cancellable) -> Result<Response> {
        let connection =
            session_bus(cancel).context("Cannot connect to the desktop session bus")?;
        let answer = connection.call_sync(
            Some("org.freedesktop.DBus"),
            "/org/freedesktop/DBus",
            "org.freedesktop.DBus",
            "ListNames",
            None,
            Some(&<(Vec<String>,)>::static_variant_type()),
            DBusCallFlags::NONE,
            CALL_TIMEOUT_MS,
            Some(cancel),
        )?;
        let (mut names,) = answer
            .get::<(Vec<String>,)>()
            .context("Invalid desktop session bus reply")?;
        names.retain(|name| name.starts_with(NAME_PREFIX));
        names.sort_unstable();

        let mut selected = None;
        if let Some(player) = request.player {
            if let Some(name) = named_player(&names, &player) {
                let (state, properties) = read_properties(&connection, name, cancel)?;
                selected = Some((name, state, properties));
            }
        } else {
            let mut last_error = None;
            for name in &names {
                let (state, properties) = match read_properties(&connection, name, cancel) {
                    Ok(player) => player,
                    Err(error) => {
                        last_error = Some(error);
                        if cancel.is_cancelled() {
                            break;
                        }
                        continue;
                    }
                };
                if selected
                    .as_ref()
                    .is_none_or(|(_, previous, _)| state > *previous)
                {
                    selected = Some((name, state, properties));
                }
                if state == PlaybackState::Playing {
                    break;
                }
            }
            if selected.is_none()
                && let Some(error) = last_error
            {
                return Err(error.context("Cannot inspect desktop players"));
            }
        }

        let action = match request.action {
            MediaPlayerAction::Status => {
                return Ok(Response::MediaPlayer(selected.map(
                    |(name, state, properties)| status(name, state, &properties),
                )));
            }
            action => action,
        };
        let (name, _, _) = selected.context("No matching desktop player")?;
        let (method, parameters) = match action {
            MediaPlayerAction::Play => ("Play", None),
            MediaPlayerAction::Pause => ("Pause", None),
            MediaPlayerAction::PlayPause => ("PlayPause", None),
            MediaPlayerAction::Stop => ("Stop", None),
            MediaPlayerAction::Next => ("Next", None),
            MediaPlayerAction::Previous => ("Previous", None),
            MediaPlayerAction::Seek { offset_us } => ("Seek", Some((offset_us,).to_variant())),
            MediaPlayerAction::Status => unreachable!("status handled above"),
        };
        connection
            .call_sync(
                Some(name),
                OBJECT,
                PLAYER_INTERFACE,
                method,
                parameters.as_ref(),
                Some(VariantTy::UNIT),
                DBusCallFlags::NO_AUTO_START,
                CALL_TIMEOUT_MS,
                Some(cancel),
            )
            .context("Desktop player rejected the command")?;
        Ok(Response::Ok)
    }

    fn named_player<'a>(names: &'a [String], player: &str) -> Option<&'a String> {
        if player.starts_with(NAME_PREFIX) {
            return names.iter().find(|name| *name == player);
        }
        let exact = format!("{NAME_PREFIX}{player}");
        let instance = format!("{exact}.");
        names
            .iter()
            .find(|name| **name == exact)
            .or_else(|| names.iter().find(|name| name.starts_with(&instance)))
    }

    fn status(name: &str, state: PlaybackState, properties: &Properties) -> MediaPlayerStatus {
        let metadata = read::<Properties>(properties, "Metadata").unwrap_or_default();
        let artwork_url = read::<String>(&metadata, "mpris:artUrl").filter(|url| url.len() <= 8192);
        let artwork_path = artwork_url
            .as_deref()
            .and_then(|url| glib::filename_from_uri(url).ok())
            .filter(|(path, host)| {
                path.is_absolute() && host.as_ref().is_none_or(|host| host == "localhost")
            })
            .and_then(|(path, _)| path.into_os_string().into_string().ok());
        let can_control = read::<bool>(properties, "CanControl").unwrap_or(false);
        let supports = |key| can_control && read::<bool>(properties, key).unwrap_or(false);
        MediaPlayerStatus {
            player: name.to_owned(),
            state,
            track_id: read::<ObjectPath>(&metadata, "mpris:trackid").map(|id| bounded_text(&id)),
            title: read::<String>(&metadata, "xesam:title").map(|text| bounded_text(&text)),
            artists: read::<Vec<String>>(&metadata, "xesam:artist")
                .unwrap_or_default()
                .into_iter()
                .take(32)
                .map(|text| bounded_text(&text))
                .collect(),
            album: read::<String>(&metadata, "xesam:album").map(|text| bounded_text(&text)),
            position_us: read::<i64>(properties, "Position").filter(|value| *value >= 0),
            duration_us: read::<i64>(&metadata, "mpris:length").filter(|value| *value >= 0),
            artwork_url,
            artwork_path,
            can_play: supports("CanPlay"),
            can_pause: supports("CanPause"),
            can_next: supports("CanGoNext"),
            can_previous: supports("CanGoPrevious"),
            can_seek: supports("CanSeek"),
        }
    }

    fn read<T: FromVariant>(properties: &Properties, key: &str) -> Option<T> {
        properties.get(key)?.get()
    }

    fn bounded_text(text: &str) -> String {
        text.chars().take(1024).collect()
    }
}
