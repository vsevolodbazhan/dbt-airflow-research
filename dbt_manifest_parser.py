from pathlib import Path

import attrs
import cattrs
import orjson


@attrs.frozen
class Metadata:
    dbt_version: str


@attrs.frozen
class Manifest:
    metadata: Metadata

    @staticmethod
    def parse(manifest: Path) -> 'Manifest':
        return cattrs.structure(
            obj=orjson.loads(manifest.read_bytes()),
            cl=Manifest,
        )


if __name__ == '__main__':
    manifest = Manifest.parse(Path('tests/manifest.json'))
    print(manifest)
