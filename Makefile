help:
	@grep '^[^\.#[:space:]].*:' Makefile

fmt:
	@echo "Formatting Rust files"
	@(rustup toolchain list | ( ! grep -q nightly && echo "Toolchain 'nightly' is not installed. Please install using 'rustup toolchain install nightly'.") ) || cargo +nightly fmt

fix: fmt
	@echo "Running cargo clippy --fix"
	cargo clippy --fix --all-features --allow-dirty --allow-staged

push:
	docker buildx build --platform linux/amd64  -t quickwit/mrecordlog:0.1 . --push

