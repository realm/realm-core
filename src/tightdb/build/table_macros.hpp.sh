DIR="$(dirname "$0")"

TABLE_MACROS_HPP="$1"

TEMP_DIR="$(mktemp -d /tmp/tightdb.codegen.XXXX)" || exit 1
if python "$DIR/table_macros.hpp.py" 15 >"$TEMP_DIR/table_macros.hpp"; then
    mv "$TEMP_DIR/table_macros.hpp" "$TABLE_MACROS_HPP"
else
    if [ -e "$TABLE_MACROS_HPP" ]; then
        echo "WARNING: Failed to update '$TABLE_MACROS_HPP'"
    else
        exit 1
    fi
fi
