/**
 * Unit tests for the dashboard.html front-end logic.
 *
 * These tests use Jest to simulate a browser environment and mock the `fetch` API
 * to test the JavaScript code in the dashboard without needing to run the full
 * web server or master.
 *
 * To run these tests:
 * 1. Ensure you have Node.js installed.
 * 2. In your `dashboard/` directory, run `npm init -y`.
 * 3. Install Jest: `npm install jest`.
 * 4. Add the following to your `package.json` file: `"jest": { "testEnvironment": "jsdom" }`.
 * 5. Run the tests with the command: `jest`.
 */

// We need to simulate the DOM environment. Jest's `jsdom` test environment
// handles this automatically. We can load the HTML content and then extract the script.
const fs = require('fs');
const path = require('path');
const html = fs.readFileSync(path.resolve(__dirname, './dashboard.html'), 'utf8');

// --- Mocking the `fetch` API ---
// We use a mock function to simulate the server's response.
// This allows us to test the front-end logic in isolation.
global.fetch = jest.fn(() =>
    Promise.resolve({
        json: () => Promise.resolve({ success: true, message: 'Job submitted!', job_id: 'test-job-123' }),
    })
);

describe('Dashboard Functionality', () => {
    // Before each test, load the HTML into the JSDOM environment.
    // This gives each test a clean, isolated DOM.
    beforeEach(() => {
        document.documentElement.innerHTML = html.toString();
        // The script inside the HTML is not executed automatically by JSDOM,
        // so we need to grab and execute it manually.
        const scriptContent = document.querySelector('script').textContent;
        // eslint-disable-next-line no-eval
        eval(scriptContent);
        // Reset the mock function before each test to clear its call history.
        jest.clearAllMocks();
    });

    // --- Test: Job Submission Form ---
    test('form submission handles success response correctly', async () => {
        // Set up the mock response for the `submit_job` endpoint.
        global.fetch.mockImplementationOnce(() =>
            Promise.resolve({
                ok: true,
                json: () => Promise.resolve({ success: true, message: 'Job submitted successfully.', job_id: 'test-job-123' }),
            })
        );

        const jobForm = document.getElementById('jobForm');
        const statusMessage = document.getElementById('statusMessage');

        // Trigger the form submission event.
        jobForm.dispatchEvent(new Event('submit'));

        // Wait for the asynchronous fetch call to complete.
        await new Promise(process.nextTick);

        // Assert that fetch was called with the correct URL and method.
        expect(global.fetch).toHaveBeenCalledWith('/submit_job', expect.objectContaining({
            method: 'POST',
        }));

        // Assert that the status message is updated correctly on success.
        expect(statusMessage.textContent).toBe('Job submitted successfully: test-job-123');
        expect(statusMessage.className).toContain('text-green-600');
    });

    test('form submission handles failure response correctly', async () => {
        // Mock a failure response from the server.
        global.fetch.mockImplementationOnce(() =>
            Promise.resolve({
                ok: false,
                json: () => Promise.resolve({ success: false, message: 'Invalid payload.' }),
            })
        );

        const jobForm = document.getElementById('jobForm');
        const statusMessage = document.getElementById('statusMessage');

        // Trigger the form submission.
        jobForm.dispatchEvent(new Event('submit'));
        await new Promise(process.nextTick);

        // Assert that the status message shows the error.
        expect(statusMessage.textContent).toBe('Error: Invalid payload.');
        expect(statusMessage.className).toContain('text-red-600');
    });

    // --- Test: Live Job Status Table ---
    test('table renders jobs correctly from server data', async () => {
        // Mock a successful fetch response with some job data.
        global.fetch.mockImplementationOnce(() =>
            Promise.resolve({
                json: () => Promise.resolve({
                    "job_1": { "job_id": "job_1", "job_type": "fibonacci", "payload": 10, "status": "COMPLETED", "result": 55 },
                    "job_2": { "job_id": "job_2", "job_type": "fibonacci", "payload": 20, "status": "DISPATCHED", "result": "" }
                }),
            })
        );

        // Manually trigger the fetch function to render the table.
        // We need to call the function reference created from the evaluated script.
        const scriptContent = document.querySelector('script').textContent;
        // eslint-disable-next-line no-eval
        const fetchAndRenderFunc = eval(`(function(){ return fetchJobsAndRender; })()`);
        await fetchAndRenderFunc();

        const jobsTableBody = document.getElementById('jobsTableBody');
        const rows = jobsTableBody.querySelectorAll('tr');

        // Assert that two rows were added to the table.
        expect(rows.length).toBe(2);

        // Assert the content of the first row (COMPLETED job).
        expect(rows[0].children[0].textContent).toBe('job_1');
        expect(rows[0].children[3].textContent).toBe('COMPLETED');
        expect(rows[0].children[3].className).toContain('text-green-600');
        expect(rows[0].children[4].textContent).toBe('55');

        // Assert the content of the second row (DISPATCHED job).
        expect(rows[1].children[0].textContent).toBe('job_2');
        expect(rows[1].children[3].textContent).toBe('DISPATCHED');
        expect(rows[1].children[3].className).toContain('text-blue-500');
        expect(rows[1].children[4].textContent).toBe('...');
    });

    test('table shows "No jobs submitted yet" when no jobs are returned', async () => {
        // Mock an empty object response.
        global.fetch.mockImplementationOnce(() =>
            Promise.resolve({
                json: () => Promise.resolve({}),
            })
        );

        // Trigger the rendering logic.
        const scriptContent = document.querySelector('script').textContent;
        // eslint-disable-next-line no-eval
        const fetchAndRenderFunc = eval(`(function(){ return fetchJobsAndRender; })()`);
        await fetchAndRenderFunc();

        const jobsTableBody = document.getElementById('jobsTableBody');
        const noJobsMessage = document.getElementById('noJobsMessage');

        // Assert that the "No jobs" message is visible and the table is empty.
        expect(jobsTableBody.children.length).toBe(0);
        expect(noJobsMessage.classList).not.toContain('hidden');
    });
});
