from pathlib import Path
from typing import Iterable, cast

import attrs
from dbt.artifacts.schemas.manifest import WritableManifest as _WritableManifest
from dbt.compilation import Linker as _Linker
from dbt.contracts.graph.manifest import Manifest as _Manifest
from dbt.contracts.graph.nodes import ModelNode as _ModelNode
from dbt.graph.graph import Graph as _Graph
from dbt.graph.graph import UniqueId as _UniqueId


def _parse_manifest(manifest: Path) -> _Manifest:
    """
    Parse a manifest file and return a internal
    dbt Manifest representation.
    """
    _manifest = manifest.as_posix()
    writable_manifest = _WritableManifest.read_and_check_versions(path=_manifest)
    return _Manifest.from_writable_manifest(writable_manifest=writable_manifest)


def _get_model_nodes(manifest: _Manifest) -> dict[str, _ModelNode]:
    """
    Filter out the model nodes from the manifest.
    """
    models = {}
    for node in manifest.nodes.values():
        if isinstance(node, _ModelNode):
            models[node.unique_id] = node
    return models


def _build_graph(manifest: _Manifest) -> _Graph:
    """
    Build a dbt graph from a manifest that allows
    to retrieve the dependencies (parents/children)
    between the nodes in the manifest.
    """
    return _Linker().get_graph(manifest)


@attrs.frozen
class Reference:
    """
    A reference to a dbt model that includes the model
    itself, its parents and children.
    """

    this: str
    parents: tuple[str, ...]
    children: tuple[str, ...]


class Index(tuple[Reference, ...]):
    """
    A tuple of references to dbt mobels that
    can be instantiated from a dbt manifest.
    """

    @staticmethod
    def from_manifest(manifest: Path) -> 'Index':
        def _collect_references() -> Iterable[Reference]:
            _manifest = _parse_manifest(manifest=manifest)
            model_nodes = _get_model_nodes(manifest=_manifest)
            graph = _build_graph(manifest=_manifest)
            for node in model_nodes:
                node = cast(_UniqueId, node)
                parents = [
                    parent
                    for parent in graph.ancestors(node=node, max_depth=None)
                    if parent in model_nodes
                ]
                children = [
                    child
                    for child in graph.descendants(node=node, max_depth=None)
                    if child in model_nodes
                ]
                yield Reference(
                    this=node,
                    parents=tuple(parents),
                    children=tuple(children),
                )

        return Index(_collect_references())


if __name__ == '__main__':
    index = Index.from_manifest(manifest=Path('tests/manifest.json'))
    for reference in index:
        print(reference.this)
        print(reference.parents)
        print(reference.children)
