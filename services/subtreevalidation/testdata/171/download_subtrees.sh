#!/bin/bash

# Download subtree files script
# Downloads both subtree and subtree_data files for each ID

BASE_URL="https://teranode-eks-ttn-us-1.internal.bsvb.tech/api/v1"
TARGET_DIR="/Users/oskarsson/gitcheckout/teranode/services/subtreevalidation/testdata/171"

# Array of IDs
IDS=(
    "dc89416c1ec67010d45f9a9ca614dd17412a4f7c47b86908e5fd15473da83e6d"
    "b6b4a947cfe907494ebcb3b704d02dbc10b9e6fb647a3387ab464f3d6cb03139"
    "cbd94f44d1711f7051c14018b18e7aff32088bbcde8d62341a19465baaf7ee9f"
    "f50aae9cb5fd241bf7786ba6820aa6d6cdd86b6d806a0f98fd20de319b31ecbc"
    "d051f92a8753baccd99a465b8a145bb17cc930c6037a5e0d875f1a41ef7f4650"
    "28c97bf503cc6c4b91cecaf77df6ae4b9e0e9cb3d6c114f5d71297f9e9c62bc3"
    "536cdfcfced061ccb04e5047066c3a9d897b46792d09b549b914491f6e733435"
    "734580692a97afa0096b795fb3e9e2ab37d85f16d3bb0d91e192785b29139dfd"
    "19ce5248a4fc8d0f6d9f0864a07284584d9c355ec8e06cc5e32a572e1fbb094e"
    "b7f196af5b120a1ea5d23c92798326cdc0725eb42725b218593ef0a144caf244"
    "b26fb14111b38e301ab2eaf5cb3fbfa1719d9c486caad0a04dd89dc58b738ac3"
    "45bd07fad2a1d2c905a666525e6499d9c54b7981171568477de3383d0f7f4a01"
    "fa574c733f5e44649a1fcf138b3691d42c6be91ffd831f6b49c72a34d014f2f4"
    "003af6bba0bd26affe08f7d73560034a701c966a1ad8ffd41f557797e204bda5"
    "a5d85bb54d0f5eb238dbcb58d2aca75a467e8a0af616ec660a6023ec966e049f"
    "6112da9ad247afd97187a84790916e14c7346f1482b573000748711ad53e68a9"
    "9a0ecc664c7a7951f3f683628c2de8f2468853889bf172f9b46da2b273d8accb"
    "b0021a3f9340085f743400e956de1dbcf27596b0403199414a6e2a58d6ebc2aa"
    "ee447af4e7b858d2154c133afd768917da95c768441b43d663e5f45b3df722a9"
    "b8d13304944aa950aabc5ce8b65e5dea80fda19354192bac7e956e2caa4be48e"
    "019c58ec8ac5b6382d7b89d85402b9c28332a6916abe86af2a6b0825c279d914"
    "aedc68e762655a4cf2228b91e4f245d29492958fb8f4741ab57cc53d0b390f0c"
    "9c2e415a78bfb930f0a94aa1bd74392d7927774378b7d81286283e6954055d51"
    "b97cb889de31f95a07d926b6a6b2e9320aa4683e5a2ed5b1f9ffc9dee8a21d57"
    "83d42c75a57bf2394e123e02622c841b7c635720ae3e8a38af1ca196d3235d70"
    "4d100a209424ea21a171bb0ab7955e6325c2b3b172626da37ccceed42adc6605"
    "d3c016b5610e9afcc4901748b3213a3dce1f22c11125d94fb139b9f1ff6ab182"
    "dbfc0fd74359007d60d020057532e8fd2321b0758d775536593973a777bc0610"
    "52f5d26558f3f579ef7a6f43825923e1a9389006d1ae7e8dd3a69e821f5f32fd"
    "d1e4cb74d2b1eb2db0a137ff373f4caba6273f719e8048f6d37203298007c501"
    "84857565717f94f5df01b3c29febd3f2c759e13dd29a7a9c2b63120b9cb3fed3"
    "8f37b421ce0ded753fc17be4da1c64ca6ef51d8698894aef6a05120d9b110ef9"
)

echo "Starting download of ${#IDS[@]} subtree files..."
echo "Target directory: $TARGET_DIR"
echo

# Change to target directory
cd "$TARGET_DIR" || { echo "Error: Cannot change to directory $TARGET_DIR"; exit 1; }

# Counters
total_files=$((${#IDS[@]} * 2))
downloaded=0
failed=0

# Download function
download_file() {
    local url="$1"
    local filename="$2"
    local id="$3"
    
    if [[ -f "$filename" ]]; then
        echo "  ✓ $filename already exists, skipping"
        return 0
    fi
    
    echo "  → Downloading $filename..."
    if curl -s -f -o "$filename" "$url"; then
        echo "  ✓ $filename downloaded successfully"
        ((downloaded++))
        return 0
    else
        echo "  ✗ Failed to download $filename"
        ((failed++))
        return 1
    fi
}

# Process each ID
for i in "${!IDS[@]}"; do
    id="${IDS[$i]}"
    echo "Processing ID $((i+1))/${#IDS[@]}: $id"
    
    # Download subtree file
    download_file "${BASE_URL}/subtree/${id}" "${id}.subtree" "$id"
    
    # Download subtree_data file
    download_file "${BASE_URL}/subtree_data/${id}" "${id}.subtree_data" "$id"
    
    echo
done

echo "Download complete!"
echo "Total files expected: $total_files"
echo "Files downloaded: $downloaded"
echo "Files failed: $failed"
echo "Files already existed: $((total_files - downloaded - failed))"