use crate::main_loop::PendingContentSwitch;
use crate::queue;
use crate::renderer;
use std::collections::HashMap;
use std::path::PathBuf;

pub(crate) fn collect_pending_content_switches<I>(
    renderers: &HashMap<String, renderer::Renderer>,
    changes: I,
) -> Vec<PendingContentSwitch>
where
    I: IntoIterator<Item = (String, (PathBuf, queue::ContentType))>,
{
    let mut pending = Vec::new();
    for (name, (path, content_type)) in changes {
        let (target_width, target_height) = renderers
            .get(&name)
            .map(|renderer| (renderer.config.width, renderer.config.height))
            .unwrap_or((0, 0));
        let target_area = u64::from(target_width) * u64::from(target_height);
        pending.push(PendingContentSwitch {
            name,
            path,
            content_type,
            shared_image_target: None,
            target_width,
            target_height,
            target_area,
        });
    }
    pending
}

pub(crate) fn annotate_shared_image_targets(pending: &mut [PendingContentSwitch]) {
    let mut largest_by_path: HashMap<PathBuf, (u32, u32, u64)> = HashMap::new();
    for change in pending.iter() {
        if change.content_type != queue::ContentType::Image {
            continue;
        }
        largest_by_path
            .entry(change.path.clone())
            .and_modify(|target| {
                if change.target_area > target.2 {
                    *target = (
                        change.target_width,
                        change.target_height,
                        change.target_area,
                    );
                }
            })
            .or_insert((
                change.target_width,
                change.target_height,
                change.target_area,
            ));
    }

    for change in pending.iter_mut() {
        if change.content_type != queue::ContentType::Image {
            continue;
        }
        change.shared_image_target = largest_by_path
            .get(&change.path)
            .map(|(width, height, _area)| (*width, *height));
    }
}

pub(crate) fn sort_pending_content_switches(pending: &mut [PendingContentSwitch]) {
    pending.sort_by(
        |left, right| match (left.content_type, right.content_type) {
            (queue::ContentType::Image, queue::ContentType::Image) => left
                .path
                .cmp(&right.path)
                .then_with(|| right.target_area.cmp(&left.target_area))
                .then_with(|| left.name.cmp(&right.name)),
            (queue::ContentType::Image, _) => std::cmp::Ordering::Less,
            (_, queue::ContentType::Image) => std::cmp::Ordering::Greater,
            _ => left.name.cmp(&right.name),
        },
    );
}

pub(crate) fn ordered_pending_content_switches<I>(
    renderers: &HashMap<String, renderer::Renderer>,
    changes: I,
) -> Vec<PendingContentSwitch>
where
    I: IntoIterator<Item = (String, (PathBuf, queue::ContentType))>,
{
    let mut pending = collect_pending_content_switches(renderers, changes);
    annotate_shared_image_targets(&mut pending);
    sort_pending_content_switches(&mut pending);
    pending
}
