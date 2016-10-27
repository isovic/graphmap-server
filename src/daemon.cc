/*
 * daemon.cc
 *
 *  Created on: Aug 30, 2014
 *      Author: ivan
 */

#include "daemon.h"

Daemon::Daemon() {
  run_ = false;
  files_to_process_.clear();
  is_dry_run_ = false;
  queue_sem_ = createSemaphore(1);
  active_sem_ = createSemaphore(0);
  terminate_ = false;
}

Daemon::~Daemon() {
}

Daemon& Daemon::GetInstance() {
    static Daemon    instance;  // Guaranteed to be destroyed.
    return instance;
}

void SigCallback(int sig) {
  Daemon::GetInstance().set_run(false);
  Daemon::GetInstance().terminate_ = true;
  Daemon::GetInstance().active_sem_->post();
}

void Daemon::Run(std::string watch_folder, std::string output_folder, std::string task_extension, bool is_dry_run, GraphMap *graphmap, const ProgramParameters &parameters) {

  struct stat st;

  if(stat(watch_folder.c_str() ,&st) != 0) {
    fprintf (stderr, "ERROR: Folder '%s' does not exist! Exiting.\n", watch_folder.c_str());
    fflush (stderr);
    exit (1);
  }

  watch_folder_ = watch_folder;
  output_folder_ = output_folder;
  is_dry_run_ = is_dry_run;
  task_extension_ = task_extension;

  run_ = true;
  terminate_ = false;
  graphmap_ = graphmap;
  parameters_ = parameters;

  // Handle the SIGINT callback.
  signal(SIGINT, SigCallback);

  if (parameters.daemon_skip_existing == false) {
    PopulateQueueFromFolder_(watch_folder_);
  }

  if (files_to_process_.size() > 0) {
    for (int32_t i=0; i<files_to_process_.size(); i++) {
      active_sem_->post();
    }
    queue_sem_->post();
  }

  // Run a separate thread for executing jobs on files.
  std::thread thread_job(&Daemon::RunJobs_, this);
  // Run the Inotifier process.
  RunNotifier_();
  // Join the threads.
  thread_job.join();
}

void Daemon::PopulateQueueFromFolder_(std::string folder_path) {
  std::vector<std::string> files;
  GetFileList_(folder_path, files);

  if (files.size() > 2) {
    fprintf (stderr, "[PopulateQueueFromFolder_] Watch folder '%s' contains unprocessed files. Adding these files to queue:\n", folder_path.c_str());
    fflush (stderr);
  }

  for (uint64_t i=0; i<files.size(); i++) {
    if (files[i] == "." || files[i] == "..")
      continue;

    fprintf (stderr, "[PopulateQueueFromFolder_]  [%ld] %s\n", i, files[i].c_str());
    fflush (stderr);
    files_to_process_.push_back(files[i]);
  }
  fprintf (stderr, "\n");
  fflush (stderr);
}

void Daemon::RunNotifier_() {
  fd_set watch_set;
  FileMonitorType file_monitor;

  char buffer[EVENT_BUF_LEN];

  int fd = inotify_init();

  if (fd < 0) {
      fprintf (stderr, "[RunNotifier_] ERROR: inotify_init!\n" );
      fflush (stderr);
      run_ = false;
  }

  FD_ZERO(&watch_set);
  FD_SET(fd, &watch_set);

  inotify_add_watch(fd, watch_folder_.c_str(), WATCH_FLAGS);

  // Enable the SIGINT signal interrupt.
  struct sigaction sa;
  sigset_t emptyset, blockset;
  sigemptyset(&blockset);
  sigaddset(&blockset, SIGINT);
  sigprocmask(SIG_BLOCK, &blockset, NULL);
  sa.sa_handler = SigCallback;
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGINT, &sa, NULL);

  fprintf (stderr, "[RunNotifier_] Running the INotify loop.\n");
  fflush (stderr);

  while (run_ == true) {
      // The command pselect waits on a file descriptor. Concretely, we wait intul one or more events
      // are reported by the Inotify API. Also, the emptyset is used to handle the SIGINT signal.
      int ready = pselect((fd + 1), &watch_set, NULL, NULL, NULL, &emptyset);

      if (ready == -1)
        continue;

      // Read event(s) from non-blocking inotify fd (non-blocking specified in inotify_init1 above).
      int length = read( fd, buffer, EVENT_BUF_LEN );
      if (length < 0) {
          perror( "read" );
      }

      // Loop through the event buffer.
      for ( int i=0; i<length; ) {
        if (run_ == false)
          break;

          struct inotify_event *event = ( struct inotify_event * ) &buffer[ i ];
          // Never actually seen this
          if ( event->wd == -1 ) {
            fprintf (stderr, "[RunNotifier_] ERROR: Overflow 1!\n" );
            fflush (stderr);
          }
          // Never seen this either
          if ( event->mask & IN_Q_OVERFLOW ) {
            fprintf (stderr, "[RunNotifier_] ERROR: Overflow 2!\n" );
            fflush (stderr);
          }

          bool add_event = false;
          std::string event_name_string = std::string(event->name);

          if ( event->len ) {
              if ( event->mask & IN_IGNORED ) {

              }
              if ( event->mask & IN_CREATE ) {
                if (!(event->mask & IN_ISDIR)) {
                    file_monitor[event_name_string] = 1;
                  }

              } else if (event->mask & IN_MOVED_TO) {
                if (!(event->mask & IN_ISDIR)) {
                  add_event = true;
//                    files_to_process_.push_back(event_name_string);
                }

              } else if ( event->mask & IN_CLOSE_WRITE ) {
                if (!(event->mask & IN_ISDIR)) {
                  FileMonitorType::iterator it = file_monitor.find(std::string(event_name_string));
                  if (it != file_monitor.end()) {
                    if ((it->second) > 0) {
                      add_event = true;
//                      files_to_process_.push_back(event_name_string);
                    }
                  }

                  file_monitor[event_name_string] = 0;
                }
              } else if ( event->mask & IN_CLOSE_NOWRITE ) {

              }
          }

          if (add_event) {
            queue_sem_->wait();
            files_to_process_.push_back(event_name_string);
            queue_sem_->post();
            active_sem_->post();
          }

          i += EVENT_SIZE + event->len;
      }
  }

  fprintf (stderr, "\n");
  fprintf (stderr, "[GraphMapDaemon] Exited thread for monitoring file system operations.\n");
  fflush (stderr);

  close(fd);
}

bool Daemon::StringEndsWith_(std::string const &full_string, std::string const &ending) {
  if (full_string.length() >= ending.length()) {
    return (0 == full_string.compare (full_string.length() - ending.length(), ending.length(), ending));
  } else {
    return false;
  }
}

std::string Daemon::TrimString_(std::string &input_string) {
  std::string ret = input_string;

  size_t found = ret.find_first_not_of(" \t");
  if (found != std::string::npos) {
    ret.erase(0, found);
  }

  found = ret.find_last_not_of(" \t");
  if (found != std::string::npos) {
    ret.erase((found + 1));
  }

  return ret;
}

//void Daemon::ParseTaskFile_(std::string task_file_path) {
//  std::string line = "";
//  std::ifstream task_file(task_file_path.c_str());
//  if (!task_file.is_open()) {
//    fprintf (stderr, "ERROR: Could not open file '%s' for reading!\n", task_file_path.c_str());
//    fflush (stderr);
//    return;
//  }
//
//  std::map<std::string, std::string> parameters;
//
//  while (getline(task_file, line)) {
//    std::size_t found = line.find(":");
//    if (found != std::string::npos) {
//      std::string param_name = line.substr(0, found);
//      std::string param_value = line.substr((found + 1));
//
//      param_name = TrimString_(param_name);
//      param_value = TrimString_(param_value);
//
//      parameters[param_name] = param_value;
//    }
//  }
//
//  task_file.close();
//
//  fprintf (stderr, "Parameters from file '%s':\n", task_file_path.c_str());
//  fflush (stderr);
//  std::map<std::string, std::string>::iterator it;
//  for (it=parameters.begin(); it!=parameters.end(); it++) {
//    printf ("'%s': '%s'\n", it->first.c_str(), it->second.c_str());
//    fflush (stdout);
//  }
//  printf ("\n");
//  fflush (stdout);
//}

std::string Daemon::GetUTCTime_() {
  char outstr[200];
  time_t t;
  struct tm *tmp;
  const char* fmt = "%a, %d %b %y %T %z";

  t = time(NULL);
  tmp = gmtime(&t);
  if (tmp == NULL) {
    fprintf (stderr, "ERROR: gmtime returned with error!\n");
    fflush (stderr);
    return std::string("[no_time]");
  }

  if (strftime(outstr, sizeof(outstr), fmt, tmp) == 0) {
    fprintf (stderr, "ERROR: Problem formatting time into string!\n");
    fflush (stderr);
    return std::string("[no_time]");
  }

  return std::string(outstr);
}

bool Daemon::GetFileList_(std::string folder, std::vector<std::string> &ret_files) {
  ret_files.clear();

  DIR *dir;
  struct dirent *ent;
  if ((dir = opendir(folder.c_str())) != NULL) {
    // Get the list of file and folder names.
    while ((ent = readdir(dir)) != NULL) {
      ret_files.push_back(std::string(ent->d_name));
    }
    closedir (dir);

  } else {
    fprintf (stderr, "ERROR: Folder '%s' not found!\n", folder.c_str());
    fflush (stderr);
    return false;
  }

  return true;
}

void Daemon::RunJobs_() {
  std::string valid_extension = task_extension_;

  fprintf (stderr, "[RunJobs_] Thread for processing jobs initialized.\n");
  fflush (stderr);

  while (run_ == true) {
    active_sem_->wait();

    if (terminate_ == true) {
      break;
    }

    queue_sem_->wait();

    std::string file_name = files_to_process_.front();
    files_to_process_.pop_front();

    queue_sem_->post();

    if (terminate_ == true) {
      break;
    }

    if (StringEndsWith_(file_name, valid_extension)) {
      ProcessSingleJob_(file_name);
    }
  }

  fprintf (stderr, "[GraphMapDaemon] Exited thread for running jobs.\n");
  fflush (stderr);
}

void Daemon::ProcessSingleJob_(std::string &file_name) {
  std::string output_file = "";

  std::stringstream ss;
  ss << "Running job: " << watch_folder_ << " " << output_folder_ << " " << file_name;
  fprintf (stderr, "[RunJobs_] %s\n", GetUTCTime_().c_str());
  fflush (stderr);

  if (is_dry_run_ == false) {
    int system_return_value = 0;

    clock_t time_start = clock();
    std::string reads_file = watch_folder_ + "/" + file_name;
    std::string sam_file = output_folder_ + "/" + file_name + ".sam";
    graphmap_->RunOnFile(parameters_, reads_file, sam_file, time_start);

    fprintf (stderr, "[RunJobs_] Finished processing job '%s'!\n", file_name.c_str());
    fprintf (stderr, "[RunJobs_] %s\n", GetUTCTime_().c_str());
    fprintf (stderr, "====================================================\n");
    fprintf (stderr, "[RunJobs_] Waiting for the next job.\n");
    fflush (stderr);
  }
}

bool Daemon::is_run() const {
  return run_;
}

void Daemon::set_run(bool run) {
  run_ = run;
}
