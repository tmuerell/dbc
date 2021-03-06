#!/bin/bash

set -euo pipefail

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        BUILD="build-linux"
elif [[ "$OSTYPE" == "darwin"* ]]; then
        BUILD="build-macos"
elif [[ "$OSTYPE" == "cygwin" ]]; then
        # POSIX compatibility layer and Linux environment emulation for Windows
        BUILD=""
elif [[ "$OSTYPE" == "msys" ]]; then
        # Lightweight shell and GNU utilities compiled for Windows (part of MinGW)
        BUILD=""
elif [[ "$OSTYPE" == "win32" ]]; then
        # I'm not sure this can happen.
        BUILD=""
elif [[ "$OSTYPE" == "freebsd"* ]]; then
        # ...
        BUILD=""
else
        # Unknown.
        BUILD=""
fi

if [[ -z "$BUILD" ]]; then
    echo "ERROR: Cannot determine your platform from '$OSTYPE'."
    exit -1
fi
echo "This script will put the 'dbc' binary to $HOME/bin. Make sure to add it to your path. Enter to continue or CTRL-C to cancel..."
read < /dev/tty

URL="https://nightly.link/tmuerell/dbc/workflows/$BUILD/main/debug-binary.zip"
FN="debug-binary.zip"
BN="dbc"

if [[ -e $FN ]]; then
    echo "ERROR: '$FN' exists. Please remove before continueing"
    exit -2
fi

echo "Ok. Downloading binary..."
curl -L -s -o "$FN" $URL
unzip -q $FN && rm $FN
chmod +x $BN
mkdir -p $HOME/bin
mv $BN $HOME/bin

echo "Installation done."
