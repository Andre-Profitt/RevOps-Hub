"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import {
  getCustomerHealthSummary,
  getCustomerHealth,
  getExpansionOpportunities,
  getChurnRiskAccounts,
} from "@/lib/foundry";
import {
  CustomerHealthSummary,
  CustomerHealth,
  ExpansionOpportunity,
  ChurnRiskAccount,
} from "@/types";

export default function CustomerHealthPage() {
  const [summary, setSummary] = useState<CustomerHealthSummary | null>(null);
  const [customers, setCustomers] = useState<CustomerHealth[]>([]);
  const [expansionOpps, setExpansionOpps] = useState<ExpansionOpportunity[]>([]);
  const [churnRisks, setChurnRisks] = useState<ChurnRiskAccount[]>([]);
  const [activeTab, setActiveTab] = useState<"overview" | "expansion" | "churn">("overview");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      try {
        const [summaryData, customerData, expansionData, churnData] = await Promise.all([
          getCustomerHealthSummary(),
          getCustomerHealth(),
          getExpansionOpportunities(),
          getChurnRiskAccounts(),
        ]);
        setSummary(summaryData);
        setCustomers(customerData);
        setExpansionOpps(expansionData);
        setChurnRisks(churnData);
      } catch (error) {
        console.error("Failed to load customer health data:", error);
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
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);

  const getHealthColor = (tier: string) => {
    switch (tier) {
      case "Healthy":
        return "bg-green-100 text-green-800";
      case "Monitor":
        return "bg-yellow-100 text-yellow-800";
      case "At Risk":
        return "bg-orange-100 text-orange-800";
      case "Critical":
        return "bg-red-100 text-red-800";
      default:
        return "bg-gray-100 text-gray-800";
    }
  };

  const getChurnRiskColor = (risk: string) => {
    switch (risk) {
      case "Low":
        return "text-green-600";
      case "Medium":
        return "text-yellow-600";
      case "High":
        return "text-orange-600";
      case "Critical":
        return "text-red-600";
      default:
        return "text-gray-600";
    }
  };

  const getUrgencyBadge = (urgency: string) => {
    switch (urgency) {
      case "Immediate":
        return "bg-red-100 text-red-800 animate-pulse";
      case "This Week":
        return "bg-orange-100 text-orange-800";
      case "This Month":
        return "bg-yellow-100 text-yellow-800";
      default:
        return "bg-gray-100 text-gray-800";
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-gray-500">Loading customer health data...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 py-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Customer Health & Expansion</h1>
              <p className="text-sm text-gray-500 mt-1">
                Track customer health, identify expansion opportunities, and manage churn risk
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
          <div className="grid grid-cols-1 md:grid-cols-5 gap-4 mb-8">
            <div className="bg-white rounded-lg shadow p-5">
              <p className="text-sm text-gray-500">Total Customers</p>
              <p className="text-2xl font-bold text-gray-900">{summary.totalCustomers}</p>
              <p className="text-xs text-gray-400 mt-1">
                Avg Health: {summary.avgHealthScore.toFixed(0)}
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-5">
              <p className="text-sm text-gray-500">Total ARR</p>
              <p className="text-2xl font-bold text-gray-900">
                {formatCurrency(summary.totalArr)}
              </p>
              <p className="text-xs text-green-600 mt-1">
                {formatCurrency(summary.healthyArr)} healthy
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-5">
              <p className="text-sm text-gray-500">At Risk ARR</p>
              <p className="text-2xl font-bold text-red-600">
                {formatCurrency(summary.atRiskArr)}
              </p>
              <p className="text-xs text-gray-400 mt-1">
                {summary.atRiskCount + summary.criticalCount} accounts
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-5">
              <p className="text-sm text-gray-500">Expansion Potential</p>
              <p className="text-2xl font-bold text-green-600">
                {formatCurrency(summary.totalExpansionPotential)}
              </p>
              <p className="text-xs text-gray-400 mt-1">
                {summary.highExpansionCount} high-potential
              </p>
            </div>
            <div className="bg-white rounded-lg shadow p-5">
              <p className="text-sm text-gray-500">Net Retention Forecast</p>
              <p className="text-2xl font-bold text-purple-600">
                {formatPercent(summary.netRetentionForecast)}
              </p>
              <p className="text-xs text-gray-400 mt-1">
                {summary.renewals90d} renewals in 90d
              </p>
            </div>
          </div>
        )}

        {/* Health Distribution */}
        {summary && (
          <div className="bg-white rounded-lg shadow p-6 mb-8">
            <h3 className="text-lg font-semibold mb-4">Health Distribution</h3>
            <div className="flex items-center gap-2">
              <div
                className="h-8 bg-green-500 rounded-l flex items-center justify-center text-white text-sm font-medium"
                style={{
                  width: `${(summary.healthyCount / summary.totalCustomers) * 100}%`,
                }}
              >
                {summary.healthyCount > 0 && `${summary.healthyCount} Healthy`}
              </div>
              <div
                className="h-8 bg-yellow-500 flex items-center justify-center text-white text-sm font-medium"
                style={{
                  width: `${(summary.monitorCount / summary.totalCustomers) * 100}%`,
                }}
              >
                {summary.monitorCount > 0 && `${summary.monitorCount} Monitor`}
              </div>
              <div
                className="h-8 bg-orange-500 flex items-center justify-center text-white text-sm font-medium"
                style={{
                  width: `${(summary.atRiskCount / summary.totalCustomers) * 100}%`,
                }}
              >
                {summary.atRiskCount > 0 && `${summary.atRiskCount} At Risk`}
              </div>
              <div
                className="h-8 bg-red-500 rounded-r flex items-center justify-center text-white text-sm font-medium"
                style={{
                  width: `${(summary.criticalCount / summary.totalCustomers) * 100}%`,
                }}
              >
                {summary.criticalCount > 0 && `${summary.criticalCount} Critical`}
              </div>
            </div>
          </div>
        )}

        {/* Tabs */}
        <div className="bg-white rounded-lg shadow">
          <div className="border-b border-gray-200">
            <nav className="flex -mb-px">
              <button
                onClick={() => setActiveTab("overview")}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === "overview"
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700"
                }`}
              >
                Customer Overview
              </button>
              <button
                onClick={() => setActiveTab("expansion")}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === "expansion"
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700"
                }`}
              >
                Expansion Opportunities
              </button>
              <button
                onClick={() => setActiveTab("churn")}
                className={`px-6 py-4 text-sm font-medium border-b-2 ${
                  activeTab === "churn"
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700"
                }`}
              >
                Churn Risk
              </button>
            </nav>
          </div>

          <div className="p-6">
            {/* Customer Overview Tab */}
            {activeTab === "overview" && (
              <div>
                <h3 className="text-lg font-semibold mb-4">All Customers</h3>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Account
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Segment
                        </th>
                        <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          ARR
                        </th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">
                          Health Score
                        </th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">
                          Health Tier
                        </th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">
                          Churn Risk
                        </th>
                        <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Days to Renewal
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200">
                      {customers.map((customer) => (
                        <tr key={customer.accountId} className="hover:bg-gray-50">
                          <td className="px-4 py-4">
                            <div className="text-sm font-medium text-gray-900">
                              {customer.accountName}
                            </div>
                            <div className="text-xs text-gray-500">{customer.industry}</div>
                          </td>
                          <td className="px-4 py-4 text-sm text-gray-600">
                            {customer.segment}
                          </td>
                          <td className="px-4 py-4 text-sm text-right font-medium text-gray-900">
                            {formatCurrency(customer.currentArr)}
                          </td>
                          <td className="px-4 py-4 text-center">
                            <div className="flex items-center justify-center">
                              <div className="w-16 bg-gray-200 rounded-full h-2 mr-2">
                                <div
                                  className={`h-2 rounded-full ${
                                    customer.healthScore >= 80
                                      ? "bg-green-500"
                                      : customer.healthScore >= 60
                                      ? "bg-yellow-500"
                                      : customer.healthScore >= 40
                                      ? "bg-orange-500"
                                      : "bg-red-500"
                                  }`}
                                  style={{ width: `${customer.healthScore}%` }}
                                />
                              </div>
                              <span className="text-sm font-medium">{customer.healthScore}</span>
                            </div>
                          </td>
                          <td className="px-4 py-4 text-center">
                            <span
                              className={`px-2 py-1 text-xs font-medium rounded-full ${getHealthColor(
                                customer.healthTier
                              )}`}
                            >
                              {customer.healthTier}
                            </span>
                          </td>
                          <td
                            className={`px-4 py-4 text-center text-sm font-medium ${getChurnRiskColor(
                              customer.churnRisk
                            )}`}
                          >
                            {customer.churnRisk}
                          </td>
                          <td className="px-4 py-4 text-sm text-right text-gray-600">
                            {customer.daysToRenewal}d
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Expansion Tab */}
            {activeTab === "expansion" && (
              <div>
                <h3 className="text-lg font-semibold mb-4">Expansion Opportunities</h3>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Account
                        </th>
                        <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Current ARR
                        </th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">
                          Expansion Score
                        </th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">
                          Tier
                        </th>
                        <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          Potential
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Recommended Play
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200">
                      {expansionOpps.map((opp) => (
                        <tr key={opp.accountId} className="hover:bg-gray-50">
                          <td className="px-4 py-4">
                            <div className="text-sm font-medium text-gray-900">
                              {opp.accountName}
                            </div>
                            <div className="text-xs text-gray-500">
                              Health: {opp.healthScore}
                            </div>
                          </td>
                          <td className="px-4 py-4 text-sm text-right text-gray-600">
                            {formatCurrency(opp.currentArr)}
                          </td>
                          <td className="px-4 py-4 text-center">
                            <div className="flex items-center justify-center">
                              <div className="w-16 bg-gray-200 rounded-full h-2 mr-2">
                                <div
                                  className="h-2 rounded-full bg-blue-500"
                                  style={{ width: `${opp.expansionScore}%` }}
                                />
                              </div>
                              <span className="text-sm font-medium">{opp.expansionScore}</span>
                            </div>
                          </td>
                          <td className="px-4 py-4 text-center">
                            <span
                              className={`px-2 py-1 text-xs font-medium rounded-full ${
                                opp.expansionTier === "High"
                                  ? "bg-green-100 text-green-800"
                                  : opp.expansionTier === "Medium"
                                  ? "bg-blue-100 text-blue-800"
                                  : "bg-gray-100 text-gray-800"
                              }`}
                            >
                              {opp.expansionTier}
                            </span>
                          </td>
                          <td className="px-4 py-4 text-sm text-right font-medium text-green-600">
                            +{formatCurrency(opp.expansionPotential)}
                          </td>
                          <td className="px-4 py-4 text-sm text-gray-600 max-w-xs">
                            {opp.recommendedPlay}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Churn Risk Tab */}
            {activeTab === "churn" && (
              <div>
                <h3 className="text-lg font-semibold mb-4">At-Risk Accounts</h3>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Account
                        </th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">
                          Risk Level
                        </th>
                        <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                          ARR at Risk
                        </th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">
                          Save Prob.
                        </th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">
                          Urgency
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Risk Factors
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Recommended Action
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200">
                      {churnRisks.map((risk) => (
                        <tr key={risk.accountId} className="hover:bg-gray-50">
                          <td className="px-4 py-4">
                            <div className="text-sm font-medium text-gray-900">
                              {risk.accountName}
                            </div>
                            <div className="text-xs text-gray-500">
                              Renewal in {risk.daysToRenewal}d
                            </div>
                          </td>
                          <td className="px-4 py-4 text-center">
                            <span
                              className={`px-2 py-1 text-xs font-medium rounded-full ${
                                risk.churnRisk === "Critical"
                                  ? "bg-red-100 text-red-800"
                                  : "bg-orange-100 text-orange-800"
                              }`}
                            >
                              {risk.churnRisk}
                            </span>
                          </td>
                          <td className="px-4 py-4 text-sm text-right font-medium text-red-600">
                            {formatCurrency(risk.arrAtRisk)}
                          </td>
                          <td className="px-4 py-4 text-sm text-center text-gray-600">
                            {formatPercent(risk.saveProbability)}
                          </td>
                          <td className="px-4 py-4 text-center">
                            <span
                              className={`px-2 py-1 text-xs font-medium rounded-full ${getUrgencyBadge(
                                risk.urgency
                              )}`}
                            >
                              {risk.urgency}
                            </span>
                          </td>
                          <td className="px-4 py-4 text-xs text-gray-600 max-w-[150px]">
                            {risk.riskFactors}
                          </td>
                          <td className="px-4 py-4 text-sm text-gray-600 max-w-[200px]">
                            {risk.recommendedAction}
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
      </main>
    </div>
  );
}
