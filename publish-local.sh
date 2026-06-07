#!/bin/bash
set -e
cd "$(dirname "$0")"

current=$(grep -oE '[0-9]+\.[0-9]+\.[0-9]+' src/Directory.Build.props | head -1)
IFS='.' read -ra parts <<< "$current"

case "${1:-patch}" in
    patch) version="${parts[0]}.${parts[1]}.$((parts[2] + 1))" ;;
    minor) version="${parts[0]}.$((parts[1] + 1)).0" ;;
    major) version="$((parts[0] + 1)).0.0" ;;
    *)     version="$1" ;;
esac

echo "→ Packing $version (was $current)"

sed -i '' "s/<Version>$current<\/Version>/<Version>$version<\/Version>/" src/Directory.Build.props
sed -i '' "s/Current version: $current\./Current version: $version./" docs/claude/fsharp-style.md

dotnet pack src/FSCommandFramework.Core/FSCommandFramework.Core.fsproj      --output ./nupkgs -c Release -p:Version=$version
dotnet pack src/FSCommandFramework.Http/FSCommandFramework.Http.fsproj       --output ./nupkgs -c Release -p:Version=$version
dotnet pack src/FSCommandFramework.Postgres/FSCommandFramework.Postgres.fsproj --output ./nupkgs -c Release -p:Version=$version

echo "→ Published $version to ./nupkgs"
