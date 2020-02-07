REPO_DIR="$(cd "$(dirname $0)"; pwd)"
POSTAL_DATA_DIR="$(cd "$REPO_DIR/.."; pwd)/libpostal_data"
POSTAL_INSTALL_DIR="$(cd "$REPO_DIR/.."; pwd)/usr/local/"
POSTAL_LIB_DIR="$POSTAL_INSTALL_DIR/lib"
POSTAL_INC_DIR="$POSTAL_INSTALL_DIR/include"

echo "REPO_DIR: $REPO_DIR"
echo "POSTAL_DATA_DIR: $POSTAL_DATA_DIR"
echo "POSTAL_INSTALL_DIR: $POSTAL_INSTALL_DIR"

mkdir "$REPO_DIR/data"
mkdir "$REPO_DIR/processed"
mkdir "$REPO_DIR/results"

cd "$REPO_DIR/.."
git clone https://github.com/openvenues/libpostal
cd libpostal
./bootstrap.sh
./configure --datadir=$POSTAL_DATA_DIR --prefix=$POSTAL_INSTALL_DIR
make -j4
make install

pip install postal --user --global-option=build_ext --global-option="-L$POSTAL_LIB_DIR" --global-option="-I$POSTAL_INC_DIR"

BASHRC_PATH="$(cd "~"; pwd)/.bashrc"
if test -f "$BASHRC_PATH"; then
    echo -e "export LD_LIBRARY_PATH=/home/aribal/usr/local/lib\n\n$(cat $BASHRC_PATH)" > $BASHRC_PATH
else
    echo -e "export LD_LIBRARY_PATH=/home/aribal/usr/local/lib\n\n" > $BASHRC_PATH
fi

cd $REPO_DIR
pip install --user -r requirements.txt

source $BASHRC_PATH

echo "idp-ws19 setup is complete!"
