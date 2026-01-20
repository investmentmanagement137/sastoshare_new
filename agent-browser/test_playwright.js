
import { chromium } from 'playwright';

console.log('Launching Playwright Chromium...');
try {
    const browser = await chromium.launch();
    console.log('Browser launched!');
    const page = await browser.newPage();
    await page.goto('https://example.com');
    console.log('Page title:', await page.title());
    await browser.close();
    console.log('Success!');
} catch (e) {
    console.error('Playwright Error:', e);
}
