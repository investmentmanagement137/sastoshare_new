
import { BrowserManager } from './dist/browser.js';

async function main() {
    console.log('Starting Diagnostic Agent...');
    const browser = new BrowserManager();

    try {
        // Launch browser (HEADED to try triggering Cloudflare clearance)
        await browser.launch({ headless: false });

        // Get the active page (BrowserManager launches with one page)
        const page = browser.getPage();

        // 1. Main Page
        const url = 'https://nepsealpha.com/mutual-fund-navs';
        console.log(`\n--- Inspecting: ${url} ---`);
        await page.goto(url, { waitUntil: 'domcontentloaded' });

        // Wait for table to load (NepseAlpha is dynamic)
        console.log('Waiting 5s for dynamic content...');
        await new Promise(r => setTimeout(r, 5000));

        // Get Snapshot (Accessibility Tree)
        console.log('Capturing Accessibility Snapshot...');
        // Note: getSnapshot returns { tree, refs }
        const { tree, refs } = await browser.getSnapshot({
            interactive: true,
            compact: true
        });

        // Analyze Structure
        console.log('\n--- Snapshot Overview ---');
        console.log(`Snapshot Size: ${tree.length} characters`);
        console.log(`Interactive Elements (Refs): ${Object.keys(refs).length}`);

        const refKeys = Object.keys(refs);
        console.log('\n--- Sample Elements (First 10) ---');
        refKeys.slice(0, 10).forEach(key => {
            const el = refs[key];
            console.log(`[${key}] Role: ${el.role}, Name: "${el.name}"`);
        });

        // Check for "Fund" links specifically
        const fundLinks = refKeys.filter(key => {
            const el = refs[key];
            // Fund names usually don't have spaces and are 3-6 chars long in the table, 
            // OR they might be full names. Let's look for known symbols.
            const knownSymbols = ['C30MF', 'NBF2', 'SIGS2'];
            return el.role === 'link' && knownSymbols.includes(el.name);
        });

        console.log(`\n--- Known Fund Links Found: ${fundLinks.length} ---`);
        if (fundLinks.length > 0) {
            fundLinks.forEach(key => {
                console.log(`Found: ${refs[key].name} (Ref: ${key})`);
            });
        } else {
            console.log("No exact match for known symbols. Listing first 10 links:");
            refKeys.filter(k => refs[k].role === 'link').slice(0, 10).forEach(k => {
                console.log(`Link: ${refs[k].name}`);
            });
        }

        await browser.close();
        console.log('\nDiagnostic Complete.');

    } catch (error) {
        console.error('Diagnostic Failed:', error);
        try { await browser.close(); } catch { }
    }
}

main();
