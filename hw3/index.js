const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");

const app = express();

app.use(bodyParser.json());

app.get("/lengthCounts", async (req, res) => {
  try {
    const data = fs.readFileSync("ouput/part-00000", "utf8");
    // console.log(data);
    const lines = data.split("\n");
    const map = new Map();
    for (const line of lines) {
      if (line.length === 0) {
        continue;
      }
      const [key, val] = line.replace(/\s+/g, "").match(/\d+/g);
      map.set(parseInt(key), parseInt(val));
    }
    console.log(map);
    const result = Object.fromEntries(map);
    return res.json(result);
  } catch (err) {
    console.error(err);
    return res.send(500);
  }
});

let http = require("http").Server(app);
const PORT = 80;

http.listen(PORT, function () {
  console.log("start listen");
});
