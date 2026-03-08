// app.js
// Dashboard Logic for Supermarket Pricing Project

const COLORS = {
    primary: '#8b5cf6',
    secondary: '#3b82f6',
    accent1: '#10b981',
    accent2: '#f59e0b',
    border: 'rgba(255, 255, 255, 0.1)',
    text: '#94a3b8'
};

const STORE_COLORS = {
    "Al-Fatah": COLORS.primary,
    "Chase Up": COLORS.secondary,
    "Metro": COLORS.accent1
};

// Global Chart Instances to allow reloading
let charts = {};
let globalData = {
    storeMetrics: [],
    categoryLdi: [],
    correlations: []
};

document.addEventListener('DOMContentLoaded', () => {
    initDashboard();
});

async function initDashboard() {
    try {
        const [storeMetrics, categoryLdi, correlations] = await Promise.all([
            fetchCSV('../data/reports/3.2_3.3_store_metrics_and_LDI.csv'),
            fetchCSV('../data/reports/3.3_category_wise_LDI.csv'),
            fetchCSV('../data/reports/3.4_correlations.csv')
        ]);

        console.log("Loaded Data:", { storeMetrics, categoryLdi, correlations });

        globalData.storeMetrics = storeMetrics;
        globalData.categoryLdi = categoryLdi;
        globalData.correlations = correlations;

        populateKPIs(storeMetrics);
        renderLdiDonut(storeMetrics);
        renderStoreMetricsChart(storeMetrics);
        renderCategoryLdiChart(categoryLdi);
        populateCorrelations(correlations);

        setupInteractivity();

    } catch (err) {
        console.error("Failed to load dashboard data", err);
        alert("Failed to load analytics data. Ensure you are running this via a web server (e.g., python -m http.server) and that the reports exist in data/reports/.");
    }
}

// Helper: Parse CSV using PapaParse
function fetchCSV(url) {
    return new Promise((resolve, reject) => {
        Papa.parse(url, {
            download: true,
            header: true,
            dynamicTyping: true,
            skipEmptyLines: true,
            complete: (results) => resolve(results.data),
            error: (err) => reject(err)
        });
    });
}

function populateKPIs(data) {
    if (!data || data.length === 0) return;

    // Find market leader (Highest LDI)
    const sorted = [...data].sort((a, b) => b.LDI - a.LDI);
    const leader = sorted[0];

    document.getElementById('kpi-leader-name').textContent = leader.Store;
    document.getElementById('kpi-leader-ldi').textContent = (leader.LDI * 100).toFixed(1);

    // Total stores monitored
    document.getElementById('kpi-store-count').textContent = data.length;

    // Lowest Volatility
    const sortedVol = [...data].sort((a, b) => a.Volatility_Score - b.Volatility_Score);
    document.getElementById('kpi-lowest-vol').textContent = sortedVol[0].Store;
    document.getElementById('kpi-lowest-vol-score').textContent = sortedVol[0].Volatility_Score.toFixed(3);

    // Compute total matched products from sum of Lowest_Price_Count / LDI roughly, 
    // or we can just sum the Lowest_Price_Count if one product has exactly one winner.
    let totalMatchedApprox = 0;
    data.forEach(s => totalMatchedApprox += s.Lowest_Price_Count);
    document.getElementById('kpi-total-matched').textContent = totalMatchedApprox.toLocaleString();
}

function renderLdiDonut(data) {
    const ctx = document.getElementById('ldiDonutChart').getContext('2d');

    const labels = data.map(d => d.Store);
    const values = data.map(d => (d.LDI * 100).toFixed(1));
    const bgColors = labels.map(label => STORE_COLORS[label] || COLORS.accent2);

    charts.ldiDonut = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: bgColors,
                borderWidth: 0,
                hoverOffset: 10
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '75%',
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: COLORS.text, font: { family: 'Inter', size: 12 } }
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            return ` ${context.label}: ${context.raw}% Market Dominance`;
                        }
                    }
                }
            }
        }
    });
}

function renderStoreMetricsChart(data) {
    const ctx = document.getElementById('storeMetricsChart').getContext('2d');

    const labels = data.map(d => d.Store);
    const volData = data.map(d => d.Volatility_Score);
    const priceIndexData = data.map(d => d.Avg_Category_Price_Index);

    charts.storeMetrics = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Price Volatility',
                    data: volData,
                    backgroundColor: COLORS.secondary,
                    borderRadius: 6,
                    yAxisID: 'y'
                },
                {
                    label: 'Category Price Index',
                    data: priceIndexData,
                    backgroundColor: COLORS.primary,
                    borderRadius: 6,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { labels: { color: COLORS.text } }
            },
            scales: {
                x: {
                    grid: { color: COLORS.border, drawBorder: false },
                    ticks: { color: COLORS.text }
                },
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
                    grid: { color: COLORS.border, drawBorder: false },
                    ticks: { color: COLORS.text }
                },
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    grid: { drawOnChartArea: false },
                    ticks: { color: COLORS.text }
                }
            }
        }
    });
}

function renderCategoryLdiChart(data) {
    if (!data || data.length === 0) {
        const ctx = document.getElementById('categoryLdiChart').getContext('2d');
        if (charts.categoryLdi) charts.categoryLdi.destroy();
        charts.categoryLdi = new Chart(ctx, { type: 'bar', data: { labels: [], datasets: [] }, options: { responsive: true, maintainAspectRatio: false } });
        return;
    }
    // Process data to grouped format
    // Store -> Category -> Value
    const categories = [...new Set(data.map(d => d.Category))].slice(0, 10); // limit to top 10
    const stores = globalData.categoryLdi && globalData.categoryLdi.length > 0 ? [...new Set(globalData.categoryLdi.map(d => d.Store))] : [...new Set(data.map(d => d.Store))];

    const datasets = stores.map(store => {
        return {
            label: store,
            backgroundColor: STORE_COLORS[store] || COLORS.accent2,
            data: categories.map(cat => {
                const record = data.find(d => d.Category === cat && d.Store === store);
                return record ? (record.Category_LDI * 100) : 0;
            }),
            borderRadius: 4
        };
    });

    const ctx = document.getElementById('categoryLdiChart').getContext('2d');

    charts.categoryLdi = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: categories.map(c => c.length > 15 ? c.substring(0, 15) + '...' : c),
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'top', labels: { color: COLORS.text } },
                tooltip: {
                    callbacks: {
                        label: (ctx) => ` ${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%`
                    }
                }
            },
            scales: {
                x: {
                    stacked: true,
                    grid: { display: false },
                    ticks: { color: COLORS.text, maxRotation: 45, minRotation: 45 }
                },
                y: {
                    stacked: true,
                    grid: { color: COLORS.border },
                    ticks: { color: COLORS.text },
                    max: 100
                }
            }
        }
    });
}

function populateCorrelations(data) {
    if (!data || data.length === 0) return;

    const container = document.getElementById('correlations-container');
    container.innerHTML = ''; // clear loading

    const record = data[0]; // expecting 1 row

    const mappings = [
        { key: 'Size_vs_Dispersion_Corr', title: 'Asset Size vs Volatility', desc: 'Correlation between pack size and price spread.' },
        { key: 'Competitors_vs_Spread_Corr', title: 'Competition effect', desc: 'How competitor count impacts price clustering.' },
        { key: 'Brand_Tier_vs_Volatility_Corr', title: 'Premium Brand Variance', desc: 'Are premium brands more stable?' }
    ];

    mappings.forEach(m => {
        let val = record[m.key];
        let colorClass = 'medium';
        if (Math.abs(val) > 0.5) colorClass = 'high';
        else if (Math.abs(val) < 0.2) colorClass = 'low';

        val = (val !== null && val !== undefined) ? val.toFixed(3) : 'N/A';

        const html = `
            <div class="correlation-item">
                <div class="corr-info">
                    <h4>${m.title}</h4>
                    <p>${m.desc}</p>
                </div>
                <div class="corr-score ${colorClass}">${val}</div>
            </div>
        `;
        container.insertAdjacentHTML('beforeend', html);
    });
}

function setupInteractivity() {
    // 1. Sidebar smooth scrolling & active state
    const navItems = document.querySelectorAll('.nav-item');

    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();

            navItems.forEach(nav => nav.classList.remove('active'));
            item.classList.add('active');

            const targetId = item.getAttribute('data-target');
            if (targetId) {
                const targetElement = document.getElementById(targetId);
                if (targetElement) {
                    targetElement.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            }
        });
    });

    // 2. Search filtering (Updates Category chart)
    const searchInput = document.getElementById('searchInput');
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            const term = e.target.value.toLowerCase();

            const filteredCategoryData = globalData.categoryLdi.filter(d =>
                d.Category && d.Category.toLowerCase().includes(term)
            );

            if (charts.categoryLdi) {
                charts.categoryLdi.destroy();
            }
            renderCategoryLdiChart(filteredCategoryData);
        });
    }

    // 3. Export button
    const exportBtn = document.getElementById('exportBtn');
    if (exportBtn) {
        exportBtn.addEventListener('click', () => {
            alert("Exporting Market Overview report... (This feature connects to your backend pipeline)");
        });
    }
}
