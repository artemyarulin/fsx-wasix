WASMER ?= wasmer
REGISTRY ?= wasmer.io
WASIX_ARTIFACT := target/wasm32-wasmer-wasi/release/fsx-wasix.rustc.wasm
ORACLE_LENGTHS ?= 1 2 3 4 5
ORACLE_ASSET_DIR := package-assets
ORACLE_CACHE_DIR := oracle-cache
PACKAGE_OUT ?= /tmp/fsx-wasix-local.webc

.PHONY: deploy build build-wasix bump-version update-index-metadata prepare-oracle-reports package-build

deploy:
	$(MAKE) prepare-oracle-reports
	$(MAKE) bump-version
	$(MAKE) update-index-metadata
	$(MAKE) build-wasix
	$(WASMER) deploy --registry $(REGISTRY)

build:
	$(MAKE) prepare-oracle-reports
	$(MAKE) bump-version
	$(MAKE) update-index-metadata
	$(MAKE) build-wasix
	$(MAKE) package-build

build-wasix:
	cargo wasix build --release
	test -s "$(WASIX_ARTIFACT)"

prepare-oracle-reports:
	cargo build --release
	mkdir -p "$(ORACLE_ASSET_DIR)" "$(ORACLE_CACHE_DIR)"
	set -eu; \
	key="$$(cargo run --quiet -- --oracle-catalog-key)"; \
	echo "oracle catalog key: $$key"; \
	for len in $(ORACLE_LENGTHS); do \
		cache="$(ORACLE_CACHE_DIR)/native-oracle-$$key-l$$len.bin"; \
		asset="$(ORACLE_ASSET_DIR)/native-oracle-l$$len.bin"; \
		root="/tmp/fsx-native-oracle-$$key-l$$len"; \
		if test -s "$$cache"; then \
			echo "reuse $$cache -> $$asset"; \
			cp "$$cache" "$$asset"; \
		else \
			echo "generate native oracle len=$$len -> $$cache"; \
			rm -rf "$$root"; \
			target/release/fsx-wasix --oracle -N "$$len" --oracle-output "$$cache" "$$root"; \
			rm -rf "$$root"; \
			cp "$$cache" "$$asset"; \
		fi; \
	done

bump-version:
	python3 -c 'from pathlib import Path; import re, time; path = Path("wasmer.toml"); text = path.read_text(); version = time.strftime("0.%Y%m%d.%H%M%S", time.gmtime()); text, count = re.subn(r"(?m)^version = \"[^\"]+\"$$", f"version = \"{version}\"", text, count=1); assert count == 1, "failed to update package version in wasmer.toml"; path.write_text(text); print(f"wasmer.toml version = {version}")'

update-index-metadata:
	python3 -c 'from pathlib import Path; import html, re, subprocess; toml = Path("wasmer.toml").read_text(); version = re.search(r"(?m)^version = \"([^\"]+)\"$$", toml).group(1); syscalls = subprocess.check_output(["target/release/fsx-wasix", "--oracle-catalog-syscalls"], text=True).strip(); path = Path("index.html"); text = path.read_text(); text = re.sub(r"(<code id=\"build_version\"[^>]*>).*?(</code>)", lambda m: m.group(1) + html.escape(version) + m.group(2), text, count=1, flags=re.S); text = re.sub(r"(<div id=\"syscall_summary\"[^>]*>).*?(</div>)", lambda m: m.group(1) + "\n                " + html.escape(syscalls) + "\n              " + m.group(2), text, count=1, flags=re.S); text = re.sub(r"(id=\"output\"[\s\S]*?value=\")[^\"]*(\")", rf"\1/data/fsx-oracle-ws/oracle-report-l3-v{html.escape(version)}.bin\2", text, count=1); path.write_text(text); print(f"index.html version = {version}"); print(f"index.html syscalls = {syscalls}")'

package-build:
	rm -f "$(PACKAGE_OUT)"
	$(WASMER) package build -o "$(PACKAGE_OUT)"
	test -s "$(PACKAGE_OUT)"
	ls -lh "$(PACKAGE_OUT)"
