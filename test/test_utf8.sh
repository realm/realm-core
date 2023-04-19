input="$(cat)"
bin2hex()
{
    python -c 'import sys;import binascii;print binascii.hexlify("".join(sys.stdin.readlines()))'
}
echo -n "UTF-8: "
printf "%s" "$input" | bin2hex
echo -n "UTF-16: "
printf "%s" "$input" | iconv --from UTF-8 --to UTF-16BE | bin2hex
