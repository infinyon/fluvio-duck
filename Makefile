OSX_BUILD_UNIVERSAL_FLAG=
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif


BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 ${OSX_BUILD_UNIVERSAL_FLAG}

pull:
	#git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	cargo clean

debug:pull
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORIES=../../fluvio-duck -B. && \
	cmake --build . --config Debug

release:
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DCMAKE_BUILD_TYPE=RelWithDebInfo ${BUILD_FLAGS} ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORIES=../../fluvio-duck -B. && \
	cmake --build . --config Release

install-clippy:
	rustup component add clippy

install-fmt:
	rustup component add rustfmt

	
check-clippy:	install-clippy
	cargo clippy --all-features

check-fmt:	install-fmt
	cargo fmt -- --check


