#include <vector>
#include <iostream>
#include <string>
#include <optional>
#include <thread>
#include <sstream>
#include <mutex>
#include <atomic>
#include <chrono>

class Debugger
{
protected:
    std::vector<std::pair<std::string, std::unique_ptr<int>>> tracked_ints;
    std::vector<std::pair<std::string, std::unique_ptr<std::string>>> tracked_strings;
    std::vector<std::pair<std::string, std::unique_ptr<std::optional<std::string>>>> tracked_opt_strings;

    static std::mutex cout_mtx;
    static std::atomic<bool> fast_execution;

    // DOES NOT WORK
    std::optional<std::string> tryInput ()
    {
        // Allows user to input commands while program is running
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

        int next = std::cin.peek();

        if (next == EOF || next == '\n') {
            std::cin.clear();  // Clear any error flags

            return std::nullopt;
        }

        // Process the input if it exists
        std::string input;
        std::getline(std::cin, input);

        return std::optional<std::string> (input);
    }

    bool disableDebugger = false;

public:

    Debugger() {}
    ~Debugger() {}
    
    void trackVariable(const std::string name, const int &var)
    {
        // Variable name and value
        tracked_ints.push_back(std::make_pair(name, std::make_unique<int>(&var)));
    }

    void trackVariable(const std::string name, const std::string &var)
    {
        tracked_strings.push_back(std::make_pair(name, std::make_unique<std::string>(&var)));
    }

    void trackVariable(const std::string name, const std::optional<std::string> &var)
    {
        tracked_opt_strings.push_back(std::make_pair(name, std::make_unique<std::optional<std::string>>(&var)));
    }

    void timer_fast_execution(long long ms)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        fast_execution = false;
    }

    void stopPoint()
    {
        bool end = false;

        if (!disableDebugger)
        {
            cout_mtx.lock();
            
            while (!end)
            {
                if (!fast_execution)
                {    
                    std::string input;
                    std::cout << "Press any key to continue..." << std::endl;
                    std::cin >> input;


                    if (input == "p")
                    {
                        end = true;
                        printVariables();
                    }
                    else if (input == "i")
                    {
                        printVariables();
                    }
                    else if (input[0] == 'f' && input.size() > 1)
                    {
                        fast_execution = true;
                        end = true;

                        std::string ms = input.substr(1, input.size() - 1);
                        std::thread th;

                        if (ms[0] != '*')   // With * it will run forever
                        {
                            try {
                                th = std::thread(&Debugger::timer_fast_execution, this, std::stoll(ms));
                                th.detach();
                            }
                            catch (std::invalid_argument& e) {
                                fast_execution = false;
                                end = false;
                                printControls();
                            }
                            catch (std::out_of_range& e) {
                                fast_execution = false;
                                end = false;
                                printControls();
                            }
                        }
                    }
                    else if (input == "c")
                    {
                        end = true;
                    }
                    else
                        printControls();
                }
                else
                {
                    printVariables();
                    end = true;

                    /*
                    if (tryInput())
                    {
                        fast_execution = false;
                    }
                    else
                    {
                        end = true;
                    }
                    */
                }
            }
            
            cout_mtx.unlock();
        }
    }

    void printVariables()
    {
        std::string output = "\n********************* DEBUGGER VARIABLES *********************\n";

        // Converts thread id to string with stringstream
        std::stringstream s_id;
        s_id << std::this_thread::get_id();
        
        output += "Thread ID: " + s_id.str() + "\n";

        output += "Int variables: \n";
        for (auto element : tracked_ints)
        {
            output += "\t" + element.first + " = " + std::to_string(*element.second) + "\n";
        }

        output += "\nString variables: \n";
        for (auto element : tracked_strings)
        {
            output += "\t" + element.first + " = " + *element.second + "\n";
        }

        for (auto element : tracked_opt_strings)
        {
            output += "\t" + element.first + " = " + element.second->value_or("NULL OPTIONAL") + "\n";
        }

        output += "******************* END DEBUGGER VARIABLES *******************\n\n";

        std::cout << output;
    }

    void printControls()
    {
        std::string output = "------ Controls of the debugger ------\n";

        output += "\t'p' - Print variables\n";
        output += "\t'c' - Continue execution\n";

        output += "--------------------------------------\n";

        std::cout << output;
    }

};