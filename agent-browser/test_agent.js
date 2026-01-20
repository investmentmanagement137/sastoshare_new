
import { BrowserManager } from './dist/browser.js';

async function main() {
    console.log('Starting BrowserManager...');
    const browser = new BrowserManager();

    try {
        console.log('Launching browser...');
        await browser.launch({ headless: true });

        console.log('Navigating to example.com...');
        await browser.navigate('https://example.com');

        console.log('Getting page text...');
        const result = await browser.getText();
        console.log('Result:', result.substring(0, 100) + '...');

        await browser.close();
        console.log('Success!');
    } catch (error) {
        console.error('Error:', error);
        await browser.close();
    }
}

main();
