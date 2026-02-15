import React from "react";
import ReactDOM from "react-dom/client";
import ChartsPage from "./ChartsPage";
import "./index.css";
import "./App.css";
import "./headlines.css";
import "./charts.css";

ReactDOM.createRoot(document.getElementById("charts-root")!).render(
  <React.StrictMode>
    <ChartsPage />
  </React.StrictMode>,
);
