import { exec } from 'child_process';
import path from 'path';

// Paths for activating the environment and running the command
const dbtEnvActivatePath = path.join(__dirname, '..', 'dbt-env', 'bin', 'activate');
const spotifyDbtPath = path.join(__dirname, '..', 'spotify_dbt');  // The folder where dbt project is located

console.log("Activating DBT Environment: ", dbtEnvActivatePath);

// Activate dbt virtual environment
exec(`source ${dbtEnvActivatePath} && cd ${spotifyDbtPath} && dbt run`, (error, stdout, stderr) => {
    if (error) {
        console.error(`Error executing DBT command: ${error.message}`);
        return;
    }
    if (stderr) {
        console.error(`stderr: ${stderr}`);
        return;
    }
    console.log(`stdout: ${stdout}`);
});