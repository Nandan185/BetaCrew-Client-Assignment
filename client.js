const net = require("net");
const fs = require("fs");

const port = 3000;
const host = "127.0.0.1";

let receivedPackets = [];
let receivedSequences = new Set();
let missingSequences = new Set();
let allPacketsReceived = false;

// Function to send the "Stream All Packets" request (callType 1)
function streamAllPackets(client) {
  const buffer = Buffer.alloc(1);
  buffer.writeUInt8(1, 0);
  client.write(buffer);
  console.log("Stream All Packets request sent");
}

// Function to resend packets request (callType 2)
function resendPacket(seq) {
  return new Promise((resolve, reject) => {
    const client = new net.Socket();

    client.connect(port, host, () => {
      const buffer = Buffer.alloc(2);
      buffer.writeUInt8(2, 0);
      buffer.writeUInt8(seq, 1);
      client.write(buffer);
    });

    client.on("data", (data) => {
      processPacketData(data);
      client.destroy();
    });

    client.on("close", () => {
      resolve();
    });

    client.on("error", (err) => {
      reject(err);
    });
  });
}

// Function to process and validate incoming data packets
function processPacketData(data) {
  const packetSize = 17;
  let offset = 0;

  while (offset + packetSize <= data.length) {
    const packet = parseSinglePacket(data.slice(offset, offset + packetSize));
    offset += packetSize;

    if (validatePacket(packet)) {
      receivedPackets.push(packet);
      receivedSequences.add(packet.packetSequence);
    } else {
      console.error("Invalid packet data received:", packet);
    }
  }
}

// Function to parse a single packet from the data buffer
function parseSinglePacket(data) {
  return {
    symbol: data.toString("ascii", 0, 4),
    buySellIndicator: data.toString("ascii", 4, 5),
    quantity: data.readInt32BE(5),
    price: data.readInt32BE(9),
    packetSequence: data.readInt32BE(13),
  };
}

// Function to validate a packet
function validatePacket({ symbol, buySellIndicator, quantity, price, packetSequence }) {
  return (
    typeof symbol === "string" &&
    symbol.length === 4 &&
    (buySellIndicator === "B" || buySellIndicator === "S") &&
    !isNaN(quantity) &&
    !isNaN(price) &&
    !isNaN(packetSequence)
  );
}

// Function to handle missing sequences and request them
async function handleMissingSequences() {
  const maxSequence = Math.max(...receivedPackets.map((p) => p.packetSequence));

  for (let i = 1; i < maxSequence; i++) {
    if (!receivedSequences.has(i)) {
      missingSequences.add(i);
    }
  }

  if (missingSequences.size > 0) {
    console.log("Missing sequences:", [...missingSequences]);

    for (const seq of missingSequences) {
      await resendPacket(seq);
    }
  }

  writeOutputToFile();
}

// Function to write the received packets to a JSON file
function writeOutputToFile() {
  console.log("Writing output to file");
  receivedPackets.sort((a, b) => a.packetSequence - b.packetSequence);

  fs.writeFileSync("output.json", JSON.stringify(receivedPackets, null, 2));
  console.log("Output written to output.json");
}

// Function to initialize the client connection and start the process
function startClient() {
  const client = new net.Socket();

  client.connect(port, host, () => {
    console.log("Connected to the server");
    streamAllPackets(client);
  });

  client.on("data", (data) => {
    console.log("Data received from server");
    processPacketData(data);

    if (allPacketsReceived) {
      handleMissingSequences();
    }
  });

  client.on("close", () => {
    console.log("Connection closed by server");
    allPacketsReceived = true;
    handleMissingSequences();
  });

  client.on("error", (err) => {
    console.error("Error:", err);
  });
}

// Start the client
startClient()
