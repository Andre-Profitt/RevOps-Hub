"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import {
  getScenarioSummary,
  getWinRateScenarios,
  getDealSizeScenarios,
  getCycleTimeScenarios,
} from "@/lib/foundry";
import {
  ScenarioSummary,
  WinRateScenario,
  DealSizeScenario,
  CycleTimeScenario,
} from "@/types";

export default function ScenarioModelingPage() {
  const [summary, setSummary] = useState<ScenarioSummary | null>(null);
  const [winRateScenarios, setWinRateScenarios] = useState<WinRateScenario[]>([]);
  const [dealSizeScenarios, setDealSizeScenarios] = useState<DealSizeScenario[]>([]);
  const [cycleTimeScenarios, setCycleTimeScenarios] = useState<CycleTimeScenario[]>([]);
  const [activeTab, setActiveTab] = useState<"win-rate" | "deal-size" | "cycle-time">("win-rate");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      try {
        const [summaryData, winRate, dealSize, cycleTime] = await Promise.all([
          getScenarioSummary(),
          getWinRateScenarios(),
          getDealSizeScenarios(),
          getCycleTimeScenarios(),
        ]);
        setSummary(summaryData);
        setWinRateScenarios(winRate);
        setDealSizeScenarios(dealSize);
        setCycleTimeScenarios(cycleTime);
      } catch (error) {
        console.error("Failed to load scenario data:", error);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  const formatCurrency = (value: number) =>
    new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);

  const formatPercent = (value: number) =>
    new Intl.NumberFormat("en-US", {
      style: "percent",
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    }).format(value);

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-gray-500">Loading scenario models...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 py-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Scenario Modeling</h1>
              <p className="text-sm text-gray-500 mt-1">
                What-if analysis for revenue forecasting
              </p>
            </div>
            <Link
              href="/"
              className="text-sm text-blue-600 hover:text-blue-800 font-medium"
            >
              Back to Dashboard
            </Link>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 py-8 sm:px-6 lg:px-8">
        {/* Summary Cards */}
        {summary && (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
            <div className="bg-white rounded-lg shadow p-6">
              <p className="text-sm text-gray-500">Baseline Revenue</p>
              <p className="text-2xl font-bold text-gray-900">
                {formatCurrency(summary.baselineRevenue)}
              </p>
              <p className="text-xs text-gray-400 mt-1">
                Win Rate: {formatPercent(summary.baselineWinRate)}
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-6">
              <p className="text-sm text-gray-500">Win Rate Upside</p>
              <p className="text-2xl font-bold text-green-600">
                +{formatCurrency(summary.winRateUpside)}
              </p>
              <p className="text-xs text-gray-400 mt-1">
                {summary.winRateBestScenario}
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-6">
              <p className="text-sm text-gray-500">Deal Size Upside</p>
              <p className="text-2xl font-bold text-blue-600">
                +{formatCurrency(summary.dealSizeUpside)}
              </p>
              <p className="text-xs text-gray-400 mt-1">
                {summary.dealSizeBestScenario}
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-6">
              <p className="text-sm text-gray-500">Total Upside Potential</p>
              <p className="text-2xl font-bold text-purple-600">
                +{formatCurrency(summary.totalUpsidePotential)}
              </p>
              <p className="text-xs text-gray-400 mt-1">Combined best-case</p>
            </div>
          </div>
        )}

        {/* Scenario Tabs */}
        <div className="bg-white rounded-lg shadow">
          <div className="border-b border-gray-200">
            <nav className="flex -mb-px">
              <button
                onClick={() => setActiveTab("win-rate")}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === "win-rate"
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700"
                }`}
              >
                Win Rate Scenarios
              </button>
              <button
                onClick={() => setActiveTab("deal-size")}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === "deal-size"
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700"
                }`}
              >
                Deal Size Scenarios
              </button>
              <button
                onClick={() => setActiveTab("cycle-time")}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === "cycle-time"
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700"
                }`}
              >
                Cycle Time Scenarios
              </button>
            </nav>
          </div>

          <div className="p-6">
            {/* Win Rate Tab */}
            {activeTab === "win-rate" && (
              <div>
                <h3 className="text-lg font-semibold mb-4">
                  Impact of Win Rate Changes
                </h3>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Scenario
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Win Rate
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Projected Revenue
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Revenue Delta
                        </th>
                        <th className="px-6 py-3 text-center text-xs font-medium text-gray-500 uppercase">
                          Impact
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200">
                      {winRateScenarios.map((scenario) => (
                        <tr
                          key={scenario.scenarioName}
                          className={
                            scenario.scenarioName === "Baseline"
                              ? "bg-blue-50"
                              : ""
                          }
                        >
                          <td className="px-6 py-4 text-sm font-medium text-gray-900">
                            {scenario.scenarioName}
                          </td>
                          <td className="px-6 py-4 text-sm text-right text-gray-600">
                            {formatPercent(scenario.projectedWinRate)}
                          </td>
                          <td className="px-6 py-4 text-sm text-right text-gray-900 font-medium">
                            {formatCurrency(scenario.projectedRevenue)}
                          </td>
                          <td
                            className={`px-6 py-4 text-sm text-right font-medium ${
                              scenario.revenueDelta > 0
                                ? "text-green-600"
                                : scenario.revenueDelta < 0
                                ? "text-red-600"
                                : "text-gray-600"
                            }`}
                          >
                            {scenario.revenueDelta > 0 ? "+" : ""}
                            {formatCurrency(scenario.revenueDelta)}
                          </td>
                          <td className="px-6 py-4 text-center">
                            <div className="w-full bg-gray-200 rounded-full h-2">
                              <div
                                className={`h-2 rounded-full ${
                                  scenario.revenueDelta >= 0
                                    ? "bg-green-500"
                                    : "bg-red-500"
                                }`}
                                style={{
                                  width: `${Math.min(
                                    100,
                                    Math.abs(scenario.revenueDeltaPct) * 100
                                  )}%`,
                                }}
                              />
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Deal Size Tab */}
            {activeTab === "deal-size" && (
              <div>
                <h3 className="text-lg font-semibold mb-4">
                  Impact of Average Deal Size Changes
                </h3>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Scenario
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Avg Deal Size
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Pipeline Value
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Projected Revenue
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Revenue Delta
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200">
                      {dealSizeScenarios.map((scenario) => (
                        <tr
                          key={scenario.scenarioName}
                          className={
                            scenario.scenarioName === "Baseline"
                              ? "bg-blue-50"
                              : ""
                          }
                        >
                          <td className="px-6 py-4 text-sm font-medium text-gray-900">
                            {scenario.scenarioName}
                          </td>
                          <td className="px-6 py-4 text-sm text-right text-gray-600">
                            {formatCurrency(scenario.projectedDealSize)}
                          </td>
                          <td className="px-6 py-4 text-sm text-right text-gray-600">
                            {formatCurrency(scenario.projectedPipeline)}
                          </td>
                          <td className="px-6 py-4 text-sm text-right text-gray-900 font-medium">
                            {formatCurrency(scenario.projectedRevenue)}
                          </td>
                          <td
                            className={`px-6 py-4 text-sm text-right font-medium ${
                              scenario.revenueDelta > 0
                                ? "text-green-600"
                                : scenario.revenueDelta < 0
                                ? "text-red-600"
                                : "text-gray-600"
                            }`}
                          >
                            {scenario.revenueDelta > 0 ? "+" : ""}
                            {formatCurrency(scenario.revenueDelta)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Cycle Time Tab */}
            {activeTab === "cycle-time" && (
              <div>
                <h3 className="text-lg font-semibold mb-4">
                  Impact of Sales Cycle Time Changes
                </h3>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Scenario
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Cycle Days
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Win Rate Impact
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Projected Revenue
                        </th>
                        <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Revenue Delta
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200">
                      {cycleTimeScenarios.map((scenario) => (
                        <tr
                          key={scenario.scenarioName}
                          className={
                            scenario.scenarioName === "Baseline"
                              ? "bg-blue-50"
                              : ""
                          }
                        >
                          <td className="px-6 py-4 text-sm font-medium text-gray-900">
                            {scenario.scenarioName}
                          </td>
                          <td className="px-6 py-4 text-sm text-right text-gray-600">
                            {scenario.projectedCycleDays} days
                          </td>
                          <td
                            className={`px-6 py-4 text-sm text-right ${
                              scenario.winRateImpact > 0
                                ? "text-green-600"
                                : scenario.winRateImpact < 0
                                ? "text-red-600"
                                : "text-gray-600"
                            }`}
                          >
                            {scenario.winRateImpact > 0 ? "+" : ""}
                            {formatPercent(scenario.winRateImpact)}
                          </td>
                          <td className="px-6 py-4 text-sm text-right text-gray-900 font-medium">
                            {formatCurrency(scenario.projectedRevenue)}
                          </td>
                          <td
                            className={`px-6 py-4 text-sm text-right font-medium ${
                              scenario.revenueDelta > 0
                                ? "text-green-600"
                                : scenario.revenueDelta < 0
                                ? "text-red-600"
                                : "text-gray-600"
                            }`}
                          >
                            {scenario.revenueDelta > 0 ? "+" : ""}
                            {formatCurrency(scenario.revenueDelta)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Insights Panel */}
        <div className="mt-8 bg-gradient-to-r from-purple-50 to-blue-50 rounded-lg p-6 border border-purple-100">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">
            Key Insights
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div>
              <p className="text-sm font-mediumCreating text-purple-700">Win Rate Lever</p>
              <p className="text-sm text-gray-600 mt-1">
                A 10% improvement in win rate could yield{" "}
                <span className="font-bold text-green-600">
                  {summary && formatCurrency(summary.winRateUpside)}
                </span>{" "}
                in additional revenue.
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-blue-700">Deal Size Lever</p>
              <p className="text-sm text-gray-600 mt-1">
                Increasing average deal size by 15% could add{" "}
                <span className="font-bold text-green-600">
                  {summary && formatCurrency(summary.dealSizeUpside)}
                </span>{" "}
                to forecast.
              </p>
            </div>
            <div>
              <p className="text-sm font-medium text-indigo-700">Cycle Time Lever</p>
              <p className="text-sm text-gray-600 mt-1">
                Reducing sales cycle by 2 weeks can improve win rates by 2% and
                increase deal velocity.
              </p>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
