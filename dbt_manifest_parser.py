from pathlib import Path
from typing import Iterable, cast

import attrs
from dbt.artifacts.schemas.manifest import WritableManifest as _WritableManifest
from dbt.compilation import Linker as _Linker
from dbt.contracts.graph.manifest import Manifest as _Manifest
from dbt.contracts.graph.nodes import ModelNode as _ModelNode
from dbt.graph.graph import Graph as _Graph
from dbt.graph.graph import UniqueId as _UniqueId


def _read_manifest(manifest: Path) -> _WritableManifest:
    """
    Read a manifest file and return a internal
    dbt manifest representation.
    """
    _manifest = manifest.as_posix()
    return _WritableManifest.read_and_check_versions(path=_manifest)


def _get_project_name(manifest: _WritableManifest) -> str:
    """
    Retrieve the project name from the manifest metadata.
    """
    project_name = manifest.metadata.project_name  # type: ignore
    if not project_name:
        raise ValueError('Project name is not defined in the manifest.')
    return project_name


def _parse_manifest(manifest: _WritableManifest) -> _Manifest:
    """
    Parse an internal dbt manifest representation
    into a dbt manifest object.
    """
    return _Manifest.from_writable_manifest(writable_manifest=manifest)


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


Selector = str


@attrs.frozen
class Reference:
    """
    A reference to a dbt model that includes the model
    itself, its parents and children.

    The models are represented as strings that can
    be used as selectors in dbt.
    """

    this: Selector
    parents: tuple[Selector, ...]
    children: tuple[Selector, ...]


class Index(tuple[Reference, ...]):
    """
    A tuple of references to dbt mobels that
    can be instantiated from a dbt manifest.
    """

    @staticmethod
    def from_manifest(manifest: Path) -> 'Index':
        writable_manifest = _read_manifest(manifest=manifest)
        _manifest = _parse_manifest(manifest=writable_manifest)
        project_name = _get_project_name(manifest=writable_manifest)

        # Unique identifier for a dbt model node
        # has the following format: 'model.<project_name>.<model_name>'.
        # To turn in into a selector-like string, we need to remove the prefix.
        def _remove_prefix(node: str) -> str:
            return node.removeprefix('model.').removeprefix(f'{project_name}.')

        def _collect_references() -> Iterable[Reference]:
            model_nodes = _get_model_nodes(manifest=_manifest)
            graph = _build_graph(manifest=_manifest)
            for node in model_nodes:
                node = cast(_UniqueId, node)
                parents = [
                    _remove_prefix(parent)
                    for parent in graph.ancestors(node=node, max_depth=None)
                    if parent in model_nodes
                ]
                children = [
                    _remove_prefix(child)
                    for child in graph.descendants(node=node, max_depth=None)
                    if child in model_nodes
                ]
                yield Reference(
                    this=_remove_prefix(node),
                    parents=tuple(parents),
                    children=tuple(children),
                )

        return Index(_collect_references())


if __name__ == '__main__':
    index = Index.from_manifest(manifest=Path('tests/manifest-prod.json'))
    for reference in index:
        print(reference.this)
        print(reference.parents)
        print(reference.children)
        print()
